from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import date, datetime, timedelta
from decimal import Decimal
import json
import os
import re
import time
import unicodedata

import pandas as pd
import requests
from dotenv import load_dotenv
from psycopg2.extras import Json, RealDictCursor


load_dotenv()
logger = LoggingMixin().log

SOURCE_POSTGRES_CONN_ID = "246PGEsteiraQBCorban"
AUX_POSTGRES_CONN_ID = "20PGEsteiraQBCorban"
DAG_ID = "etl_newcorban_envia_insert_parceiros"
SOURCE_TABLE = "newcorban_insert_parceiros"
PROPOSALS_TABLE = "newcorban_propostas"
PENDING_USERS_TABLE = "newcorban_usuarios_pendentes"
TARGET_STATUS_NAME = "Assinatura Realizada"
TARGET_STATUS_KEY = "assinatura_realizada"
PROPOSAL_INSERTION_URL = os.getenv(
    "NEWCORBAN_PROPOSAL_INSERTION_URL",
    "http://192.168.5.20:8797/insertion/proposal",
)
DEFAULT_LIMIT = int(os.getenv("NEWCORBAN_INSERT_PARCEIROS_LIMIT", "400"))
DEFAULT_DELAY_SECONDS = float(os.getenv("NEWCORBAN_INSERT_PARCEIROS_DELAY_SECONDS", "2"))
DEFAULT_TIMEOUT_SECONDS = int(os.getenv("NEWCORBAN_INSERT_PARCEIROS_TIMEOUT_SECONDS", "60"))

TIPO_PRODUTO_MAP = {
    "NOVOS": "margem_livre",
    "NOVO": "margem_livre",
    "REFINANCIAMENTO": "refinanciamento",
    "PORT COM REDUCAO": "portabilidade",
    "PORT + REFIN": "port_com_refin",
    "REFIN DA PORT": "refin_da_port",
    "CARTAO": "cartao",
    "SEGURO": "margem_livre",
}

BANCO_ALIASES = {
    "banco_inbursa_sa": "inbursa",
    "qualibanking": "quali_banking",
    "qi_tech": "quali_banking",
    "570qualibanking": "quali_banking",
    "banco_v8": "v8_bank",
}

UFS_VALIDAS = {
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS",
    "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC",
    "SP", "SE", "TO",
}


def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_source_pg():
    return PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)


def get_aux_pg():
    return PostgresHook(postgres_conn_id=AUX_POSTGRES_CONN_ID)


def valor_vazio(value) -> bool:
    return value is None or pd.isna(value) or str(value).strip().lower() in {"", "nan", "none", "nat"}


def limpar_valores(value) -> str:
    if valor_vazio(value):
        return ""
    text = str(value).strip().lower().replace(" ", "_")
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    return re.sub(r"[^\w]", "", text)


def chave_login(value) -> str | None:
    if valor_vazio(value):
        return None
    text = str(value).strip().lower()
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]", "", text)
    return text or None


def texto_ou_none(value) -> str | None:
    if valor_vazio(value):
        return None
    if isinstance(value, float) and value.is_integer():
        value = int(value)
    return str(value).strip() or None


def digitos_ou_none(value) -> str | None:
    text = texto_ou_none(value)
    if not text:
        return None
    digits = re.sub(r"\D", "", text)
    return digits or None


def int_ou_none(value) -> int | None:
    if valor_vazio(value):
        return None
    try:
        return int(float(str(value).strip().replace(",", ".")))
    except (TypeError, ValueError):
        return None


def decimal_ou_none(value):
    if valor_vazio(value):
        return None
    if isinstance(value, Decimal):
        return value
    if isinstance(value, str):
        value = value.strip().replace("R$", "").replace(" ", "")
        if "," in value and "." in value:
            value = value.replace(".", "").replace(",", ".")
        elif "," in value:
            value = value.replace(",", ".")
    try:
        return Decimal(str(value))
    except Exception:
        return None


def data_ou_none(value):
    if valor_vazio(value):
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    text = str(value).strip()
    dayfirst = not bool(re.fullmatch(r"\d{4}-\d{2}-\d{2}", text))
    parsed = pd.to_datetime(value, errors="coerce", dayfirst=dayfirst)
    if pd.isna(parsed):
        return None
    return parsed.date().isoformat()


def normalizar_cpf(value) -> str | None:
    digits = digitos_ou_none(value)
    if not digits:
        return None
    return digits.zfill(11) if len(digits) < 11 else digits


def normalizar_uf(value, default: str = "SP") -> str:
    text = texto_ou_none(value)
    if not text:
        return default
    text = text.upper()
    return text if text in UFS_VALIDAS else default


def possui_seguro(value):
    amount = decimal_ou_none(value)
    if amount is None:
        return None
    return amount > 0


def codigo_tabela(value) -> str | None:
    text = texto_ou_none(value)
    if not text:
        return None
    first = text.split(" ")[0]
    digits = re.sub(r"\D", "", first)
    if digits:
        return digits[-3:]
    return first if first else text


def payload_raw(row: dict) -> dict:
    raw = row.get("payload_raw")
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, str):
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {}


def nested_get(data: dict, *path):
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def resposta_api_para_json(response: requests.Response):
    try:
        return response.json()
    except ValueError:
        return response.text


def read_table(conn, table_name: str) -> pd.DataFrame:
    return pd.read_sql_query(f"SELECT * FROM public.{table_name}", conn)


def ensure_source_columns(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            ALTER TABLE public.{qident(SOURCE_TABLE)}
                ADD COLUMN IF NOT EXISTS processada integer,
                ADD COLUMN IF NOT EXISTS processada_retorno jsonb,
                ADD COLUMN IF NOT EXISTS payload_envio jsonb,
                ADD COLUMN IF NOT EXISTS processada_em timestamptz,
                ADD COLUMN IF NOT EXISTS processada_erro text,
                ADD COLUMN IF NOT EXISTS bloqueio_envio text,
                ADD COLUMN IF NOT EXISTS bloqueio_envio_em timestamptz;
        """)
    conn.commit()


def buscar_pendentes(conn, limit: int) -> list[dict]:
    sql = f"""
        SELECT *
        FROM public.{qident(SOURCE_TABLE)}
        WHERE COALESCE(processada, 0) NOT IN (200, 201)
          AND bloqueio_envio IS NULL
        ORDER BY captured_at ASC NULLS LAST, updated_at ASC NULLS LAST
        LIMIT %s
    """
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (limit,))
        return [dict(row) for row in cur.fetchall()]


def buscar_propostas_ja_existentes(conn, rows: list[dict]) -> set[str]:
    numeros_ade = {
        numero_ade
        for row in rows
        if (numero_ade := texto_ou_none(row.get("numeroAde")))
    }
    if not numeros_ade:
        return set()

    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT DISTINCT btrim(bank_proposal_number::text)
            FROM public.{qident(PROPOSALS_TABLE)}
            WHERE bank_proposal_number IS NOT NULL
              AND btrim(bank_proposal_number::text) = ANY(%s)
        """, (list(numeros_ade),))
        return {row[0] for row in cur.fetchall() if row[0]}


def carregar_mapas(conn) -> dict:
    banks = read_table(conn, "banks")
    covenants = read_table(conn, "covenants")
    products = read_table(conn, "products")
    statuses = read_table(conn, "proposal_statuses")
    users = read_table(conn, "users")
    teams = read_table(conn, "teams")
    franchises = read_table(conn, "franchises")

    banks["key"] = banks["name"].map(limpar_valores).replace(BANCO_ALIASES)
    covenants["key"] = covenants["name"].map(limpar_valores)
    products["key"] = products["name"].map(limpar_valores)
    statuses["key"] = statuses["name"].map(limpar_valores).str.replace("__", "_", regex=False)
    franchises["key"] = franchises["name"].map(limpar_valores)

    users["key_name"] = users["name"].map(chave_login)
    users["key_username"] = users["username"].map(chave_login) if "username" in users.columns else None
    users["team_id"] = users["team_id"].map(int_ou_none) if "team_id" in users.columns else None
    users["active_bool"] = users["active"].fillna(False).astype(bool) if "active" in users.columns else False
    users = users.sort_values(by=["active_bool"], ascending=[False])

    teams_by_id = {
        int(row["id"]): row
        for _, row in teams.iterrows()
        if not valor_vazio(row.get("id"))
    }
    franchises_by_id = {
        int(row["id"]): row
        for _, row in franchises.iterrows()
        if not valor_vazio(row.get("id"))
    }

    users_map = {}
    for _, user in users.iterrows():
        team_id = int_ou_none(user.get("team_id"))
        team = teams_by_id.get(team_id) if team_id else None
        for key in (user.get("key_username"), user.get("key_name")):
            if key and key not in users_map:
                users_map[key] = {
                    "id": int_ou_none(user.get("id")),
                    "team_id": team_id,
                    "franchise_id": int_ou_none(team.get("franchise_id")) if team is not None else int_ou_none(user.get("franchise_id")),
                    "team_name": texto_ou_none(team.get("name")) if team is not None else None,
                    "franchise_name": texto_ou_none(team.get("franchise_name")) if team is not None else None,
                    "name": user.get("name"),
                    "username": user.get("username"),
                }

    return {
        "banks": banks.drop_duplicates("key").set_index("key")["id"].to_dict(),
        "covenants": covenants.drop_duplicates("key").set_index("key")["id"].to_dict(),
        "products": products.drop_duplicates("key").set_index("key")["id"].to_dict(),
        "statuses": statuses.drop_duplicates("key").set_index("key")["id"].to_dict(),
        "target_status_id": int_ou_none(statuses.drop_duplicates("key").set_index("key")["id"].to_dict().get(TARGET_STATUS_KEY)),
        "users": users_map,
        "franchises": franchises.drop_duplicates("key").set_index("key")["id"].to_dict(),
        "franchises_by_id": franchises_by_id,
    }


def desbloquear_logins_resolvidos(source_conn, pending_conn, mapas: dict) -> None:
    found_keys = set(mapas["users"].keys())

    with pending_conn.cursor() as cur:
        cur.execute(f"""
            SELECT login_norm
            FROM public.{qident(PENDING_USERS_TABLE)}
            WHERE status = 'RESOLVIDO'
               OR (%s = true AND login_norm = ANY(%s))
        """, (bool(found_keys), list(found_keys) if found_keys else [""]))
        resolved_keys = {row[0] for row in cur.fetchall()}
        if not resolved_keys:
            return

    with source_conn.cursor() as cur:
        cur.execute(f"""
            SELECT "numeroAde", login
            FROM public.{qident(SOURCE_TABLE)}
            WHERE bloqueio_envio = 'USUARIO_NAO_ENCONTRADO'
        """)
        numeros_para_liberar = [
            row[0]
            for row in cur.fetchall()
            if chave_login(row[1]) in resolved_keys
        ]

        if numeros_para_liberar:
            cur.execute(f"""
                UPDATE public.{qident(SOURCE_TABLE)}
                SET bloqueio_envio = NULL,
                    bloqueio_envio_em = NULL,
                    processada_erro = NULL,
                    updated_at = now()
                WHERE "numeroAde" = ANY(%s)
            """, (numeros_para_liberar,))

    with pending_conn.cursor() as cur:
        cur.execute(f"""
            UPDATE public.{qident(PENDING_USERS_TABLE)}
            SET status = 'RESOLVIDO',
                resolvido_em = COALESCE(resolvido_em, now()),
                updated_at = now()
            WHERE login_norm = ANY(%s)
        """, (list(resolved_keys),))
    source_conn.commit()
    pending_conn.commit()


def registrar_usuario_pendente(conn, rows: list[dict]) -> None:
    grouped = {}
    for row in rows:
        login = texto_ou_none(row.get("login"))
        login_norm = chave_login(login)
        if not login_norm:
            continue
        item = grouped.setdefault(login_norm, {
            "login": login,
            "login_norm": login_norm,
            "nome_sugerido": login,
            "cpfs": set(),
            "numeros_ade": set(),
            "qtd": 0,
        })
        item["qtd"] += 1
        cpf = normalizar_cpf(row.get("cpf"))
        numero_ade = texto_ou_none(row.get("numeroAde"))
        if cpf:
            item["cpfs"].add(cpf)
        if numero_ade:
            item["numeros_ade"].add(numero_ade)

    with conn.cursor() as cur:
        for item in grouped.values():
            cur.execute(f"""
                INSERT INTO public.{qident(PENDING_USERS_TABLE)} (
                    login, login_norm, nome_sugerido, qtd_propostas, cpfs, numeros_ade, status, updated_at
                )
                VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, 'PENDENTE', now())
                ON CONFLICT (login_norm)
                DO UPDATE SET
                    login = EXCLUDED.login,
                    nome_sugerido = EXCLUDED.nome_sugerido,
                    qtd_propostas = EXCLUDED.qtd_propostas,
                    cpfs = EXCLUDED.cpfs,
                    numeros_ade = EXCLUDED.numeros_ade,
                    status = CASE
                        WHEN {qident(PENDING_USERS_TABLE)}.status = 'RESOLVIDO' THEN {qident(PENDING_USERS_TABLE)}.status
                        ELSE 'PENDENTE'
                    END,
                    updated_at = now();
            """, (
                item["login"],
                item["login_norm"],
                item["nome_sugerido"],
                item["qtd"],
                json.dumps(sorted(item["cpfs"]), ensure_ascii=False),
                json.dumps(sorted(item["numeros_ade"]), ensure_ascii=False),
            ))
    conn.commit()


def bloquear_linha(conn, numero_ade: str | None, motivo: str, erro: str) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE public.{qident(SOURCE_TABLE)}
            SET bloqueio_envio = %s,
                bloqueio_envio_em = now(),
                processada_erro = %s,
                updated_at = now()
            WHERE "numeroAde" = %s
        """, (motivo, erro, numero_ade))
    conn.commit()


def atualizar_retorno(conn, numero_ade: str | None, status_code: int | None, retorno, erro: str | None, payload_envio: dict | None) -> None:
    with conn.cursor() as cur:
        cur.execute(f"""
            UPDATE public.{qident(SOURCE_TABLE)}
            SET processada = %s,
                processada_retorno = %s,
                payload_envio = %s,
                processada_em = now(),
                processada_erro = %s,
                bloqueio_envio = NULL,
                bloqueio_envio_em = NULL,
                updated_at = now()
            WHERE "numeroAde" = %s
        """, (status_code, Json(retorno), Json(payload_envio), erro, numero_ade))
    conn.commit()


def produto_key(row: dict) -> str:
    tipo = texto_ou_none(row.get("tipo"))
    mapped = TIPO_PRODUTO_MAP.get((tipo or "").upper(), tipo)
    return limpar_valores(mapped)


def banco_key(row: dict) -> str:
    key = limpar_valores(row.get("bancoRefin"))
    return BANCO_ALIASES.get(key, key)


def fase_id(row: dict, mapas: dict):
    return mapas.get("target_status_id")


def montar_payload(row: dict, mapas: dict) -> tuple[dict | None, str | None]:
    raw = payload_raw(row)
    login = texto_ou_none(row.get("login"))
    user = mapas["users"].get(chave_login(login))
    if not user:
        return None, "USUARIO_NAO_ENCONTRADO"

    is_retencao = int_ou_none(row.get("retencao")) == 1
    target_status_id = None if is_retencao else fase_id(row, mapas)
    if not is_retencao and not target_status_id:
        return None, f"FASE_NAO_ENCONTRADA: {TARGET_STATUS_NAME}"

    bank_id = int_ou_none(mapas["banks"].get(banco_key(row)))
    product_id = int_ou_none(mapas["products"].get(produto_key(row)))
    covenant_id = int_ou_none(mapas["covenants"].get("inss")) or 7000
    franchise_id = user.get("franchise_id") or int_ou_none(mapas["franchises"].get(limpar_valores(row.get("plataforma"))))
    team_assignment = texto_ou_none(user.get("team_id"))
    franchise_assignment = texto_ou_none(franchise_id)

    released_amount = decimal_ou_none(row.get("valorRetorno")) or decimal_ou_none(row.get("valorEmprestimoRefin")) or Decimal("0")
    financed_amount = decimal_ou_none(row.get("valorEmprestimoRefin")) or released_amount

    phone_digits = digitos_ou_none(row.get("telefone") or nested_get(raw, "borrower", "phone"))
    phone = None
    if phone_digits and len(phone_digits) >= 10:
        phone = {
            "area_code": int(phone_digits[:2]),
            "number": phone_digits[2:],
            "extension": None,
            "type": "CELULAR",
        }

    document_number = digitos_ou_none(row.get("documento") or nested_get(raw, "borrower", "document", "number")) or normalizar_cpf(row.get("cpf")) or "000000"
    benefit = digitos_ou_none(row.get("beneficio") or nested_get(raw, "borrower", "benefit")) or "0000000000"
    numero_ade = texto_ou_none(row.get("numeroAde"))
    customer_name = (
        texto_ou_none(nested_get(raw, "borrower", "name"))
        or texto_ou_none(nested_get(raw, "borrower", "fullName"))
        or texto_ou_none(row.get("nome"))
        or texto_ou_none(row.get("card"))
        or "NAO INFORMADO"
    )
    bank_account = nested_get(raw, "creditBankAccount") or {}

    payload = {
        "proposal": {
            "bank_id": bank_id or 1,
            "released_amount": released_amount,
            "covenant_id": covenant_id,
            "product_id": product_id,
            "promoter_id": 4536,
            "origin_id": 9316,
            "table_code": codigo_tabela(row.get("nomeTabela")) or texto_ou_none(row.get("codigoTabela")),
            "term": int_ou_none(row.get("prazoRefin")),
            "rate": decimal_ou_none(row.get("taxa")),
            "financed_amount": financed_amount,
            "installment_amount": decimal_ou_none(row.get("valorParcelaRefin")),
            "iof_amount": decimal_ou_none(row.get("iof")),
            "status_id": target_status_id,
            "substatus": None if is_retencao else TARGET_STATUS_NAME,
            "srcc": None,
            "has_insurance": possui_seguro(row.get("seguro")),
            "has_tc": None,
            "is_duplicate": None,
            "registered_at": data_ou_none(row.get("dataEmissao") or row.get("dataStatus")),
            "typing_login": "nc_maste_partner@qualigrupo.com.br",
        },
        "payment": {
            "type": "CONTA_POUPANCA" if "poupanca" in limpar_valores(row.get("tipoConta")) else "CARTAO_MAGNETICO" if "cartao" in limpar_valores(row.get("tipoPagamento")) else "CONTA_CORRENTE",
            "bank_code": digitos_ou_none(row.get("codBankConta") or nested_get(bank_account, "bank", "code")),
            "account": digitos_ou_none(row.get("numeroConta") or bank_account.get("number")),
            "account_digit": digitos_ou_none(row.get("digitoConta") or bank_account.get("digit")),
            "branch": digitos_ou_none(bank_account.get("branch")),
            "branch_digit": digitos_ou_none(bank_account.get("branchDigit")),
            "pix": None,
        },
        "assignment": {
            "seller_id": user.get("id"),
            "co_seller_id": None,
            "formalizer_id": None,
            "team_id": team_assignment,
            "franchise_id": franchise_assignment,
        },
        "phone": phone,
        "address": {
            "street_number": texto_ou_none(row.get("numero") or nested_get(raw, "borrower", "address", "number")) or "S/N",
            "postal_code": digitos_ou_none(row.get("cep") or nested_get(raw, "borrower", "address", "zipCode")),
            "street": texto_ou_none(row.get("endereco") or nested_get(raw, "borrower", "address", "street")) or "NAO INFORMADO",
            "neighborhood": texto_ou_none(row.get("bairro") or nested_get(raw, "borrower", "address", "district")),
            "city": texto_ou_none(row.get("cidade") or nested_get(raw, "borrower", "address", "city")),
            "state": normalizar_uf(row.get("uf") or nested_get(raw, "borrower", "address", "state")),
            "address_complement": None,
        },
        "document": {
            "number": document_number,
            "type": "RG",
            "issue_date": None,
            "state": normalizar_uf(row.get("docEstado") or nested_get(raw, "borrower", "document", "issuingState")),
        },
        "benefit": {
            "registration_number": benefit,
            "benefit_species": int_ou_none(row.get("codigoBeneficio")) or 1,
            "covenant_id": covenant_id,
            "state": normalizar_uf(row.get("estadoBeneficio") or nested_get(raw, "borrower", "benefitState") or row.get("uf")),
            "birth_date": None,
            "benefit_dispatch_date": data_ou_none(row.get("dataInicioBeneficio")),
            "unblock_date": None,
            "margin": None,
            "card_margin": None,
            "calculation_base": None,
        },
        "bank_reference": {
            "proposal_number": numero_ade or texto_ou_none(row.get("numeroPropostaNu")) or "NAO_INFORMADO",
            "api_reference": None,
            "formalization_link": texto_ou_none(row.get("formalizacaoUrl")),
        },
        "dates": {
            "endorsement_date": None,
            "payment_date": None,
            "cancellation_date": None,
            "completion_date": None,
        },
        "contracts": [{
            "number": texto_ou_none(row.get("contratoPortado")) or texto_ou_none(row.get("contratoRefin")) or "-",
            "origin_bank_code": int_ou_none(digitos_ou_none(row.get("bancoPortado"))) or 1,
            "installment_amount": decimal_ou_none(row.get("valorParcelaPortado")),
            "outstanding_balance": decimal_ou_none(row.get("saldoDevedor")),
            "rate": None,
            "installments_total": int_ou_none(row.get("parcelaDevedor")),
            "installments_paid": None,
        }],
        "snapshot": {
            "address_number": texto_ou_none(row.get("numero") or nested_get(raw, "borrower", "address", "number")),
            "benefit_registration_number": benefit,
            "document_number": document_number,
            "phone_number": phone_digits,
        },
        "customer": {
            "cpf": normalizar_cpf(row.get("cpf") or nested_get(raw, "borrower", "identity")),
            "name": customer_name,
            "birth_date": data_ou_none(row.get("dtNascimento") or nested_get(raw, "borrower", "birthDate")),
            "gender": "FEMININO" if limpar_valores(row.get("sexo") or nested_get(raw, "borrower", "sex")).startswith("fem") else "MASCULINO" if limpar_valores(row.get("sexo") or nested_get(raw, "borrower", "sex")).startswith("masc") else "NAO_DEFINIDO",
            "marital_status": "NAO_DEFINIDO",
            "nationality": None,
            "mother_name": None,
            "father_name": None,
            "income": None,
            "email": None,
            "deceased": None,
            "illiterate": None,
            "potential": None,
            "do_not_disturb": None,
            "has_disability": None,
            "disability_description": None,
        },
    }

    if not payload["phone"]:
        payload.pop("phone", None)

    return payload, None


def json_safe(value):
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    return value


def preparar_payload_envio(payload: dict) -> dict:
    return json.loads(json.dumps(payload, default=json_safe))


def log_retorno_api_completo(response: requests.Response, numero_ade: str | None, resposta) -> None:
    corpo = (
        json.dumps(resposta, ensure_ascii=False, indent=2, default=json_safe)
        if isinstance(resposta, (dict, list))
        else response.text
    )
    logger.info(
        "[INSERT PARCEIROS] RETORNO COMPLETO numeroAde=%s\n"
        "status=%s\n"
        "reason=%s\n"
        "url=%s\n"
        "elapsed_seconds=%.3f\n"
        "headers=%s\n"
        "body=%s",
        numero_ade,
        response.status_code,
        response.reason,
        response.url,
        response.elapsed.total_seconds(),
        json.dumps(dict(response.headers), ensure_ascii=False, indent=2),
        corpo,
    )


def enviar_payload(
    payload: dict,
    timeout: int,
    numero_ade: str | None = None,
    mostrar_payload: bool = False,
) -> tuple[int | None, object, str | None, dict]:
    payload_envio = preparar_payload_envio(payload)
    if mostrar_payload:
        logger.info(
            "[INSERT PARCEIROS] JSON BODY DA PRIMEIRA REQUISICAO numeroAde=%s:\n%s",
            numero_ade,
            json.dumps(payload_envio, ensure_ascii=False, indent=2),
        )
    response = requests.post(
        PROPOSAL_INSERTION_URL,
        json=payload_envio,
        timeout=timeout,
    )
    resposta = resposta_api_para_json(response)
    log_retorno_api_completo(response, numero_ade, resposta)
    erro = None if 200 <= response.status_code < 300 else str(resposta)
    return response.status_code, resposta, erro, payload_envio


def processar_insert_parceiros(limit: int = DEFAULT_LIMIT, delay_seconds: float = DEFAULT_DELAY_SECONDS, timeout: int = DEFAULT_TIMEOUT_SECONDS, **context):
    source_pg = get_source_pg()
    aux_pg = get_aux_pg()
    source_conn = source_pg.get_conn()
    aux_conn = aux_pg.get_conn()
    pending_conn = source_conn
    source_conn.autocommit = False

    try:
        ensure_source_columns(source_conn)
        mapas = carregar_mapas(aux_conn)
        desbloquear_logins_resolvidos(source_conn, pending_conn, mapas)
        rows = buscar_pendentes(source_conn, limit)

        if not rows:
            logger.info("[INSERT PARCEIROS] Nenhuma proposta pendente para envio.")
            return

        propostas_ja_existentes = buscar_propostas_ja_existentes(source_conn, rows)
        logger.info(
            "[INSERT PARCEIROS] Verificacao previa: lote=%s ja_existentes_newcorban=%s",
            len(rows),
            len(propostas_ja_existentes),
        )

        bloqueados = []
        bloqueados_duplicidade = 0
        enviados = 0
        tentativas = 0
        for index, row in enumerate(rows, start=1):
            numero_ade = texto_ou_none(row.get("numeroAde"))

            if numero_ade in propostas_ja_existentes:
                bloquear_linha(
                    source_conn,
                    numero_ade,
                    "PROPOSTA_JA_EXISTENTE",
                    f"numeroAde {numero_ade} ja existe em {PROPOSALS_TABLE}.bank_proposal_number; insercao bloqueada para evitar duplicidade.",
                )
                bloqueados_duplicidade += 1
                logger.info(
                    "[INSERT PARCEIROS] Duplicidade bloqueada numeroAde=%s; ja existe em %s.bank_proposal_number",
                    numero_ade,
                    PROPOSALS_TABLE,
                )
                continue

            payload, bloqueio = montar_payload(row, mapas)

            if bloqueio:
                bloqueados.append(row)
                bloquear_linha(source_conn, numero_ade, bloqueio, f"Login nao encontrado em public.users: {row.get('login')}")
                logger.info("[INSERT PARCEIROS] Bloqueado numeroAde=%s login=%s motivo=%s", numero_ade, row.get("login"), bloqueio)
                continue

            logger.info(
                "[INSERT PARCEIROS] [%s/%s] Pronta para envio numeroAde=%s",
                index,
                len(rows),
                numero_ade,
            )

            if tentativas > 0 and delay_seconds:
                time.sleep(delay_seconds)
            tentativas += 1

            try:
                status_code, resposta, erro, payload_envio = enviar_payload(
                    payload,
                    timeout,
                    numero_ade,
                    mostrar_payload=(tentativas == 1),
                )
                atualizar_retorno(source_conn, numero_ade, status_code, resposta, erro, payload_envio)
                enviados += 1
                if erro:
                    logger.error(
                        "[INSERT PARCEIROS] FALHA numeroAde=%s status=%s resposta=%s",
                        numero_ade,
                        status_code,
                        json.dumps(resposta, ensure_ascii=False, default=json_safe) if isinstance(resposta, (dict, list)) else resposta,
                    )
                else:
                    logger.info("[INSERT PARCEIROS] SUCESSO numeroAde=%s status=%s", numero_ade, status_code)
            except requests.RequestException as exc:
                response = getattr(exc, "response", None)
                status_code = getattr(response, "status_code", None)
                resposta = resposta_api_para_json(response) if response is not None else str(exc)
                if response is not None:
                    log_retorno_api_completo(response, numero_ade, resposta)
                atualizar_retorno(source_conn, numero_ade, status_code, resposta, str(exc), preparar_payload_envio(payload))
                logger.exception("[INSERT PARCEIROS] Erro ao enviar numeroAde=%s", numero_ade)

        if bloqueados:
            registrar_usuario_pendente(pending_conn, bloqueados)

        logger.info(
            "[INSERT PARCEIROS] Finalizado. Lidos=%s enviados=%s bloqueados_usuario=%s bloqueados_duplicidade=%s",
            len(rows),
            enviados,
            len(bloqueados),
            bloqueados_duplicidade,
        )
    finally:
        aux_conn.close()
        source_conn.close()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description="Envia propostas assinadas de parceiros para insertion e controla logins pendentes.",
    schedule_interval="*/5 * * * *",
    start_date=days_ago(1),
     max_active_runs=1,
    catchup=False,
    tags=["newcorban", "parceiros", "insert"],
) as dag:
    processar = PythonOperator(
        task_id="processar_insert_parceiros",
        python_callable=processar_insert_parceiros,
    )
