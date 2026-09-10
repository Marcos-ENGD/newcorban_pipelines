import pytz
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator
import sys
from pathlib import Path
from airflow.utils.log.logging_mixin import LoggingMixin
import time
import os
import re
import requests
import pandas as pd
from datetime import datetime, timedelta
from dotenv import load_dotenv
import json
import uuid
import hashlib
from urllib.parse import quote
from airflow.providers.postgres.hooks.postgres import PostgresHook

ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))
logger = LoggingMixin().log
load_dotenv()

POSTGRES_CONN_ID =  "246PGEsteiraQBCorban"
FINAL_TABLE = "newcorban_insert_parceiros"
CONTRACT_NUMBER_URL = os.getenv(
    "NEWCORBAN_CONTRACT_NUMBER_URL",
    "https://integration.ajin.io/v3/loans/contract-number",
)

TIPO_MAP = {
    1: "Novos",
    2: "Refinanciamento",
    3: "Port com Reducao",
    4: "Port + Refin",
    5: "Refin da Port",
    6: "Novos",
    9: "Cartao",
    13: "Seguro",
}

FINAL_COLUMNS = [
    ("af", "text"),
    ("numeroAdePortado", "text"),
    ("tipo", "text"),
    ("tipo_id", "bigint"),
    ("fase_id", "bigint"),
    ("fase", "bigint"),
    ("retencao", "integer"),
    ("login", "text"),
    ("observacao", "text"),
    ("nota_status", "text"),
    ("descricao", "text"),
    ("plataforma", "text"),
    ("consultor", "text"),
    ("plataforma_id", "bigint"),
    ("numeroPropostaNu", "text"),
    ("contratoRefin", "text"),
    ("nomeTabela", "text"),
    ("dataContratoRefin", "timestamptz"),
    ("bancoRefin", "text"),
    ("bancoRefin_id", "bigint"),
    ("codigoTabela", "bigint"),
    ("card", "text"),
    ("prazoRefin", "bigint"),
    ("valorParcelaRefin", "numeric"),
    ("valorEmprestimoRefin", "numeric"),
    ("valorRetorno", "numeric"),
    ("dataEmissao", "timestamptz"),
    ("numeroAde", "text"),
    ("saldoDevedor", "numeric"),
    ("parcelaDevedor", "bigint"),
    ("valorParcelaPortado", "numeric"),
    ("contratoPortado", "text"),
    ("bancoPortado", "text"),
    ("dataRetornoCip", "timestamptz"),
    ("dataDevedor", "timestamptz"),
    ("retornoSaldo", "numeric"),
    ("dataRetornoCip_origem", "timestamptz"),
    ("dataRetornoCip_expectativa", "timestamptz"),
    ("dataVenctoPrimeiraParcela", "timestamptz"),
    ("dataVenctoUltimaParcela", "timestamptz"),
    ("dataInicioBeneficio", "timestamptz"),
    ("cpf", "text"),
    ("beneficio", "text"),
    ("sexo", "text"),
    ("dtNascimento", "timestamptz"),
    ("telefone", "text"),
    ("docTipo", "text"),
    ("documento", "text"),
    ("docEstado", "text"),
    ("tipoConta", "text"),
    ("codBankConta", "text"),
    ("numeroConta", "text"),
    ("digitoConta", "text"),
    ("estadoBeneficio", "text"),
    ("codigoBeneficio", "bigint"),
    ("tipoPagamento", "text"),
    ("taxa", "numeric"),
    ("formalizacaoUrl", "text"),
    ("numeroContrato", "text"),
    ("dataStatus", "timestamptz"),
    ("seguro", "numeric"),
    ("iof", "numeric"),
    ("endereco", "text"),
    ("numero", "text"),
    ("bairro", "text"),
    ("cidade", "text"),
    ("uf", "text"),
    ("cep", "text"),
    ("endorsementStatus", "bigint"),
    ("assinatura_assinada", "boolean"),
]

def qident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def get_pg_conn():
    return PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).get_conn()



def _to_int_nullable(valor):
    num = pd.to_numeric(pd.Series([valor]), errors="coerce").iloc[0]
    if pd.isna(num):
        return None
    return int(num)


def _to_bool(valor):
    if isinstance(valor, bool):
        return valor
    if valor in (1, "1", "true", "True", "TRUE"):
        return True
    if valor in (0, "0", "false", "False", "FALSE"):
        return False
    return False


def clean_sql_value(v):
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    return v


def _json_dumps(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def getenv_int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def getenv_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, default))
    except (TypeError, ValueError):
        return default


def payload_hash(payload: dict) -> str:
    payload_text = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True, default=str)
    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()


def normalize_numero_ade(value):
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def prepare_final_df(df: pd.DataFrame) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["numeroAde"] = df["numeroAde"].apply(normalize_numero_ade)
    df = df[df["numeroAde"].notna()].copy()

    if df.empty:
        return df

    df = aplicar_regras_fase(df)
    df["retencao"] = 1

    for col, sql_type in FINAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

        if sql_type in ("bigint", "integer"):
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(
                lambda x: None if pd.isna(x) else int(x)
            ).astype(object)
        elif sql_type == "numeric":
            df[col] = pd.to_numeric(df[col], errors="coerce").apply(
                lambda x: None if pd.isna(x) else float(x)
            ).astype(object)
        elif sql_type == "boolean":
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else bool(x)).astype(object)
        elif sql_type == "timestamptz":
            df[col] = pd.to_datetime(df[col], errors="coerce").apply(
                lambda x: None if pd.isna(x) else x.to_pydatetime()
            ).astype(object)
        else:
            df[col] = df[col].apply(lambda x: None if pd.isna(x) else str(x)).astype(object)

    df = (
        df.sort_values(by=["source_page_no", "source_item_no"], ascending=[False, False])
        .drop_duplicates(subset=["numeroAde"], keep="first")
    )
    df = df.astype(object).where(pd.notna(df), None)
    return df


def aplicar_regras_fase(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    if "tipo" in df.columns:
        df["tipo"] = df["tipo_id"].map(TIPO_MAP).combine_first(df["tipo"])
    else:
        df["tipo"] = df["tipo_id"].map(TIPO_MAP)

    df["fase"] = pd.to_numeric(df["fase_id"], errors="coerce")

    contratos_com_refin_63_65 = set(
        df.loc[
            (df["tipo"] == "Refin da Port")
            & (df["fase"].isin([63, 65])),
            "numeroContrato",
        ].dropna()
    )

    mask_port_forcar_65 = (
        (df["tipo"] == "Port + Refin")
        & (df["fase"].isin([63, 65]))
        & (~df["numeroContrato"].isin(contratos_com_refin_63_65))
    )

    df.loc[mask_port_forcar_65, "fase"] = 65

    fase_port_refin = (
        df.loc[
            (df["tipo"] == "Port + Refin")
            & (df["fase"].isin([36, 32, 34, 78, 65, 45, 46])),
            ["numeroContrato", "fase"],
        ]
        .drop_duplicates()
        .set_index("numeroContrato")["fase"]
        .to_dict()
    )

    mask_refin_da_port = (
        (df["tipo"] == "Refin da Port")
        & (df["numeroContrato"].isin(fase_port_refin))
        & (df["fase"] != 67)
    )
    df.loc[mask_refin_da_port, "fase"] = df.loc[mask_refin_da_port, "numeroContrato"].map(fase_port_refin)

    df.loc[df["tipo_id"] == 5, "dataRetornoCip"] = df["dataRetornoCip_origem"]
    df.loc[df["fase"] == 46, "fase"] = 38
    df["bancoRefin"] = df["bancoRefin"].replace("QI Tech", "570-QUALIBANKING")

    return df


def ensure_final_table_columns(conn):
    all_table_columns = FINAL_COLUMNS + [
        ("payload_raw", "jsonb"),
        ("payload_hash", "text"),
        ("run_uuid", "uuid"),
        ("source_page_no", "integer"),
        ("source_item_no", "integer"),
        ("source_scroll_id", "text"),
        ("source_endpoint", "text"),
        ("source_params", "jsonb"),
        ("processada", "integer"),
        ("processada_retorno", "jsonb"),
        ("processada_em", "timestamptz"),
        ("processada_erro", "text"),
        ("bloqueio_envio", "text"),
        ("bloqueio_envio_em", "timestamptz"),
    ]
    add_columns_sql = ",\n            ".join(
        f"ADD COLUMN IF NOT EXISTS {qident(col)} {sql_type}"
        for col, sql_type in all_table_columns
    )

    with conn.cursor() as cur:
        create_columns_sql = ",\n            ".join(
            f"{qident(col)} {sql_type}"
            for col, sql_type in all_table_columns
        )
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {qident(FINAL_TABLE)} (
                {create_columns_sql},
                captured_at timestamptz NOT NULL DEFAULT now(),
                updated_at timestamptz NOT NULL DEFAULT now()
            );
        """)
        cur.execute(f"ALTER TABLE {qident(FINAL_TABLE)}\n     {add_columns_sql};")
        cur.execute(f"""
            CREATE UNIQUE INDEX IF NOT EXISTS {qident(f"idx_{FINAL_TABLE}_numeroAde_unique")}
            ON {qident(FINAL_TABLE)} ({qident("numeroAde")})
            WHERE {qident("numeroAde")} IS NOT NULL;
        """)


def insert_newcorban_postgres(df: pd.DataFrame) -> int:
    df = prepare_final_df(df)
    if df.empty:
        logger.info("[INSERT NEWCORBAN] Base vazia, nada para gravar.")
        return 0

    metadata_columns = [
        ("payload_raw", "jsonb"),
        ("payload_hash", "text"),
        ("run_uuid", "uuid"),
        ("source_page_no", "integer"),
        ("source_item_no", "integer"),
        ("source_scroll_id", "text"),
        ("source_endpoint", "text"),
        ("source_params", "jsonb"),
    ]
    all_columns = [col for col, _ in FINAL_COLUMNS] + [col for col, _ in metadata_columns]
    insert_columns_sql = ", ".join(qident(col) for col in all_columns)
    placeholders = ", ".join(
        "%s::jsonb" if col in ("payload_raw", "source_params") else "%s"
        for col in all_columns
    )
    sql = f"""
    INSERT INTO {qident(FINAL_TABLE)} (
        {insert_columns_sql}
    ) VALUES (
        {placeholders}
    )
    ON CONFLICT ({qident("numeroAde")})
    WHERE {qident("numeroAde")} IS NOT NULL
    DO NOTHING;
    """

    rows = []
    for _, row in df.iterrows():
        rows.append(tuple(clean_sql_value(row.get(col)) for col in all_columns))

    conn = get_pg_conn()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        ensure_final_table_columns(conn)
        cur.executemany(sql, rows)
        affected_count = cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(rows)
        conn.commit()
        logger.info(f"[INSERT NEWCORBAN] {len(rows)} registros enviados | {affected_count} inseridos novos.")
        return affected_count
    except Exception as e:
        conn.rollback()
        logger.exception(f"[INSERT NEWCORBAN] Erro no executemany: {e}")
        raise
    finally:
        cur.close()
        conn.close()



def process_dataframe(df):
    for col in df.columns:
        if df[col].dtype == 'datetime64[ns]':
            df[col] = df[col].apply(lambda x: '' if pd.isna(x) or x == pd.NaT else x)
        else:
            df[col] = df[col].apply(lambda x: '' if pd.isna(x) or x is None else x)

    return df


def normalizar_lista_propostas(value) -> list[str]:
    if value is None:
        return []
    itens = value if isinstance(value, (list, tuple, set)) else re.split(r"[,;\n\r]+", str(value))
    resultado = []
    vistos = set()
    for item in itens:
        proposta = str(item).strip()
        if proposta and proposta not in vistos:
            vistos.add(proposta)
            resultado.append(proposta)
    return resultado


def correcao_api(params_init=None, run_uuid=None, modo_busca="filtro", propostas=None):
    load_dotenv()

    sp_tz = pytz.timezone("America/Sao_Paulo")
    now_sp = datetime.now(sp_tz)

    base_url = os.getenv("BASE_URL")
    api_key = os.getenv("TNKV_NW_RETENCAO")

    if not base_url:
        raise ValueError("BASE_URL não definido no .env")
    if not api_key:
        raise ValueError("TNKV_NW_RETENCAO  não definido no .env")

    headers = {
        "apikey": api_key,
        "Content-Type": "application/json",
    }

    api_limit = getenv_int("NEWCORBAN_API_LIMIT", 100)
    min_page_delay = getenv_float("NEWCORBAN_MIN_PAGE_DELAY_SECONDS", 4.2)
    max_page_delay = getenv_float("NEWCORBAN_MAX_PAGE_DELAY_SECONDS", 20.0)
    rate_limit_cooldown = getenv_float("NEWCORBAN_429_COOLDOWN_SECONDS", 45.0)
    max_tentativas = getenv_int("NEWCORBAN_API_MAX_TENTATIVAS", 8)
    
    #####################################################################################################################################################################
    
    params_padrao = {
        "partner.code.ne": 41,
        "SignatureDate.gte": '2026-07-27',
        "limit": api_limit,
    }
    params = dict(params_init) if isinstance(params_init, dict) else params_padrao
    params.setdefault("limit", api_limit)
    
    #####################################################################################################################################################################

    def get_retry_after_seconds(resp):
        retry_after = resp.headers.get("Retry-After")
        if not retry_after:
            return None

        try:
            return max(float(retry_after), 0)
        except ValueError:
            return None

    def fetch_with_retry(url, headers, params=None, timeout=60):
        last_body = None
        hit_rate_limit = False
        for tentativa in range(1, max_tentativas + 1):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=timeout)
            except Exception as e:
                wait_seconds = min(2 ** tentativa, max_page_delay)
                print(f"[Retry {tentativa}] exception: {e} | aguardando {wait_seconds:.1f}s...")
                time.sleep(wait_seconds)
                continue

            if resp.status_code == 200:
                try:
                    return resp.json(), hit_rate_limit
                except Exception:
                    snippet = (resp.text or "")[:800]
                    raise Exception(f"Resposta 200 mas JSON inválido. Body (parcial): {snippet}")

            if resp.status_code == 429:
                hit_rate_limit = True

            try:
                last_body = resp.json()
            except Exception:
                last_body = (resp.text or "")[:800]

            if resp.status_code == 429:
                wait_seconds = get_retry_after_seconds(resp)
                if wait_seconds is None:
                    wait_seconds = rate_limit_cooldown
            else:
                wait_seconds = min(2 ** tentativa, max_page_delay)

            print(
                f"[Retry {tentativa}] falhou ({resp.status_code}), aguardando {wait_seconds:.1f}s...\n"
                f"URL: {url}\n"
                f"Params: {params}\n"
                f"Body (parcial): {str(last_body)[:800]}"
            )
            time.sleep(wait_seconds)

        raise Exception(f"Falha após {max_tentativas} tentativas em {url}. Último body: {str(last_body)[:300]}")

    def pick_origin_contract(item: dict) -> dict:
        origin = item.get("originContract")
        if isinstance(origin, dict):
            return origin

        origins = item.get("originContracts") or []
        if isinstance(origins, list) and origins:
            first = origins[0]
            if isinstance(first, dict):
                return first

        return {}

    def safe_int_str(v):
        return None if v is None else str(v)

    def extract_rows(
        items,
        page_no: int,
        scroll_id_at_page: str,
        source_endpoint: str | None = None,
        source_params_value: dict | None = None,
    ):
        rows = []
        sem_origin = 0

        for item_no, item in enumerate(items or [], start=1):
            item = item or {}

            product = item.get("product") or {}
            operation = product.get("operation") or {}
            borrower = item.get("borrower") or {}
            borrowerBenefitT = borrower.get("benefitType") or {}
            borrowerDoc = borrower.get("document") or {}
            borrowerDocTp = borrowerDoc.get("type") or {}
            status = item.get("status") or {}
            reason = item.get("reason") or {}
            partner = item.get("partner") or {}

            conta = item.get("creditBankAccount") or {}
            contaTp = conta.get("type") or {}
            contaBank = conta.get("bank") or {}


            broker = item.get("broker") or {}
            proposal = item.get("proposal") or {}
            pprod = proposal.get("product") or {}
            ptype = pprod.get("type") or {}
            lender = item.get("lender") or {}
            rule = item.get("rule") or {}
            card = item.get("card") or {}
            card_type = card.get("type") or {}
            signature = item.get("signature") or {}
            signature_status = signature.get("status") or {}
            endors = item.get("endorsementStatus") or {}
            pay_nb = borrower.get("benefitPaymentMethod") or {}

            origin = pick_origin_contract(item)
            if not origin:
                sem_origin += 1
            address = borrower.get("address") or {}
            originBank = origin.get("lender") or {}

            validations = item.get("validations") or []
            description = None
            if isinstance(validations, list) and validations:
                first_val = validations[0] or {}
                if isinstance(first_val, dict):
                    description = first_val.get("description")


            signature_status_code = signature_status.get("code")
            signature_status_key = str(signature_status.get("key") or "").strip().lower()
            signature_status_name = str(signature_status.get("name") or "").strip().lower()


            assinatura_assinada = (
                    signature_status_code == 2 or
                    signature_status_key == "signed" or
                    signature_status_name == "assinado"
            )

            rows.append({
                "af": item.get("contractNumber"),
                "numeroAdePortado": origin.get("contractNumber"),
                "tipo": operation.get("name"),
                "tipo_id": operation.get("code"),
                "fase_id": status.get("code"),
                "observacao": reason.get("name"),
                "nota_status": status.get("note"),
                "descricao": description,
                "plataforma": partner.get("name"),
                "consultor": broker.get("name"),
                "plataforma_id": partner.get("code"),
                "numeroPropostaNu": safe_int_str(origin.get("portabilityNumber")),
                "contratoRefin": item.get("contractNumber"),
                "nomeTabela": rule.get("name"),
                "dataContratoRefin": status.get("date"),
                "bancoRefin": lender.get("name"),
                "bancoRefin_id": 554,
                "codigoTabela": rule.get("code"),
                "card": card_type.get("name"),
                "prazoRefin": item.get("term"),
                "valorParcelaRefin": item.get("installmentValue"),
                "valorEmprestimoRefin": item.get("loanValue"),
                "valorRetorno": item.get("netValue"),
                "dataEmissao": item.get("proposalDate"),
                "numeroAde": item.get("contractNumber"),
                "saldoDevedor": origin.get("dueBalanceValue"),
                "parcelaDevedor": origin.get("term"),
                "valorParcelaPortado": origin.get("installmentValue"),
                "contratoPortado": origin.get("contractNumber"),
                "bancoPortado": originBank.get("name"),
                "dataRetornoCip": item.get("dueBalanceDate"),
                "dataDevedor": item.get("dueBalanceDate"),
                "retornoSaldo": item.get("dueBalanceValue"),
                "login": item.get("accessId"),
                "dataRetornoCip_origem": origin.get("dueBalanceDate"),
                "dataRetornoCip_expectativa": origin.get("dueBalanceExpectedReturnDate"),
                "dataVenctoPrimeiraParcela": item.get("firstDueDate"),
                "dataVenctoUltimaParcela": item.get("lastDueDate"),
                "dataInicioBeneficio": borrower.get("benefitStartDate"),
                "cpf": borrower.get("identity"),
                "beneficio": borrower.get("benefit"),  
                "sexo": borrower.get("sex"),  
                "dtNascimento": borrower.get("birthDate"),
                "telefone": borrower.get("phone"),
                "docTipo": borrowerDocTp.get("name"),
                "documento": borrowerDoc.get("number"),
                "docEstado": borrowerDoc.get("issuingState"),
                "tipoConta": contaTp.get("name"),
                "codBankConta": contaBank.get("code"),
                "numeroConta": conta.get("number"),
                "digitoConta": conta.get("digit"),
                "estadoBeneficio": borrower.get("benefitState"),
                "codigoBeneficio": borrowerBenefitT.get("code"),
                "tipoPagamento": pay_nb.get("name"),
                "taxa": item.get("rate"),
                "formalizacaoUrl": signature.get("url"),
                "numeroContrato": safe_int_str(item.get("proposalNumber")),
                "dataStatus": status.get("date"),
                "seguro": item.get("insuranceValue"),
                "iof": item.get("iofValue"),
                "endereco": address.get("street"),
                "numero": address.get("number"),
                "bairro": address.get("district"),
                "cidade": address.get("city"),
                "uf": address.get("state"),
                "cep": address.get("zipCode"),
                "endorsementStatus": endors.get("code"),
                "assinatura_assinada": bool(assinatura_assinada),
                "payload_raw": _json_dumps(item),
                "payload_hash": payload_hash(item),
                "run_uuid": run_uuid,
                "source_page_no": page_no,
                "source_item_no": item_no,
                "source_scroll_id": scroll_id_at_page,
                "source_endpoint": source_endpoint or base_url,
                "source_params": _json_dumps(source_params_value if source_params_value is not None else params),
            })

        if sem_origin:
            print(f"[extract_rows] Itens sem originContract/originContracts: {sem_origin} / {len(items or [])}")

        return rows

    modo_busca = str(modo_busca or "filtro").strip().lower()
    if modo_busca == "lista":
        lista_propostas = normalizar_lista_propostas(propostas)
        if not lista_propostas:
            raise ValueError("Modo lista selecionado, mas nenhuma proposta foi informada.")

        list_delay = getenv_float("NEWCORBAN_LIST_DELAY_SECONDS", 0.5)
        linhas = []
        total = len(lista_propostas)
        logger.info("[MODO LISTA] Consultando %s proposta(s) por contract-number.", total)

        for indice, proposta in enumerate(lista_propostas, start=1):
            if indice > 1 and list_delay > 0:
                time.sleep(list_delay)
            endpoint_proposta = f"{CONTRACT_NUMBER_URL}/{quote(proposta, safe='')}"
            logger.info("[MODO LISTA] [%s/%s] proposta=%s", indice, total, proposta)
            data_proposta, _ = fetch_with_retry(endpoint_proposta, headers)

            item = data_proposta
            if isinstance(data_proposta, dict):
                for chave in ("data", "loan", "item"):
                    candidato = data_proposta.get(chave)
                    if isinstance(candidato, dict):
                        item = candidato
                        break
                if isinstance(data_proposta.get("items"), list) and data_proposta["items"]:
                    item = data_proposta["items"][0]

            if not isinstance(item, dict):
                raise ValueError(f"Resposta inesperada para proposta {proposta}: {type(item).__name__}")

            linhas.extend(extract_rows(
                [item],
                page_no=indice,
                scroll_id_at_page=None,
                source_endpoint=endpoint_proposta,
                source_params_value={"modo_busca": "lista", "proposta": proposta},
            ))

        logger.info("[MODO LISTA] Total coletado: %s", len(linhas))
        return pd.DataFrame(linhas), {
            "total_items": len(linhas),
            "total_pages": total,
            "params": {"modo_busca": "lista", "propostas": lista_propostas},
            "endpoint": CONTRACT_NUMBER_URL,
            "modo_busca": "lista",
        }

    if modo_busca != "filtro":
        raise ValueError(f"modo_busca invalido: {modo_busca}. Use 'filtro' ou 'lista'.")

    data, hit_rate_limit = fetch_with_retry(base_url, headers, params=params)
    scroll_id = data.get("scrollId")
    total_count = data.get("count", 0)

    logger.info(f"Total estimado: {total_count} registros (scroll).")

    total_pages = 0
    linhas = []

    itens0 = data.get("items", []) or []
    total_pages += 1

    linhas.extend(extract_rows(itens0, total_pages, scroll_id))

    print(f"Página 1: extraídos {len(itens0)} itens | scroll_id={(str(scroll_id)[:12] + '...') if scroll_id else None}")

    page_delay = min_page_delay

    rodada = 1

    success_streak = 0

    while True:
        rodada += 1
        time.sleep(page_delay)

        if not scroll_id:
            print("Sem scrollId retornado; encerrando.")
            break

        next_url = f"{base_url}/search/next/{scroll_id}"
        print(f"-- scroll-next #{rodada} -- url={(next_url[:70] + '...') if len(next_url) > 70 else next_url}")

        try:
            data, hit_rate_limit = fetch_with_retry(next_url, headers)
        except Exception as e:
            raise RuntimeError(f"Falha máxima na iteração #{rodada}; captura interrompida para evitar base incompleta.") from e

        if hit_rate_limit:
            success_streak = 0
            page_delay = min(max_page_delay, page_delay * 1.5)
            logger.info("[RATE LIMIT] 429 detectado; novo intervalo entre paginas: %.1fs", page_delay)
        else:
            success_streak += 1
            if success_streak >= 10 and page_delay > min_page_delay:
                page_delay = max(min_page_delay, page_delay * 0.95)
                success_streak = 0
                logger.info("[RATE LIMIT] intervalo ajustado para %.1fs apos respostas estaveis", page_delay)

        scroll_id = data.get("scrollId")
        items = data.get("items", []) or []

        print(
            f"  > itens={len(items)} | acumulado={len(linhas)} | scroll_id={(str(scroll_id)[:12] + '...') if scroll_id else None}")

        if not items:
            print("Fim do scroll: sem mais itens.")
            break

        total_pages += 1

        linhas.extend(extract_rows(items, total_pages, scroll_id))

    print(f"\nTotal de registros coletados: {len(linhas)}")
    return pd.DataFrame(linhas), {
        "total_items": len(linhas),
        "total_pages": total_pages,
        "params": params,
        "endpoint": base_url,
        "modo_busca": "filtro",
    }



def chunk_list(data, chunk_size: int):
    for i in range(0, len(data), chunk_size):
        yield data[i:i + chunk_size]



default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
        dag_id='etl_newcorban_insert_retencao',
        default_args=default_args,
        schedule_interval="*/5 * * * *",
        catchup=False,
        max_active_runs=1,
        description='Captura propostas da retenção e grava no Postgres uma vez por numeroAde',
        tags=['newcorban', 'agilus', 'postgres', 'etl'],
        params={
            "modo_busca": Param(
                "filtro",
                type="string",
                enum=["filtro", "lista"],
                title="Modo de busca",
                description="filtro = pesquisa paginada normal; lista = propostas informadas individualmente",
            ),
            "propostas": Param(
                "",
                type="string",
                title="Lista de propostas",
                description="Usado no modo lista. Informe contratos separados por virgula ou uma por linha.",
            ),
            "filtros": Param(
                {
                "user.id.in": [
                            "26ef0478-d5d9-4d3d-b7c0-ffb1183763ce",
                            "931e02a6-698f-4105-860f-590cb6fc227c",
                            "073d07ac-87ec-4da8-b5cc-b4ffa402bc96",
                        ], 
                        "limit": 20,
                },
                type="object",
                title="Filtros da busca normal",
            ),
        },
) as dag:
    def fetch_and_upsert_newcorban(**context):
        dag_run = context.get("dag_run")
        dag_params = context.get("params") or {}
        run_conf = dict(dag_run.conf or {}) if dag_run and getattr(dag_run, "conf", None) else {}
        modo_busca = run_conf.get("modo_busca", dag_params.get("modo_busca", "filtro"))
        propostas = run_conf.get("propostas", dag_params.get("propostas", ""))
        params_init = (
            run_conf.get("filtros")
            or run_conf.get("params")  # compatibilidade com execucoes antigas
            or dag_params.get("filtros")
        )

        run_uuid = str(uuid.uuid4())
        logger.info("[NEWCORBAN] Inicio run_uuid=%s modo_busca=%s", run_uuid, modo_busca)

        df_api, api_meta = correcao_api(
            params_init=params_init,
            run_uuid=run_uuid,
            modo_busca=modo_busca,
            propostas=propostas,
        )
        total_gravado = insert_newcorban_postgres(df_api)

        logger.info(
            "[NEWCORBAN] Finalizado run_uuid=%s | coletados=%s | paginas=%s | gravados=%s",
            run_uuid,
            api_meta.get("total_items"),
            api_meta.get("total_pages"),
            total_gravado,
        )

        return {
            "run_uuid": run_uuid,
            "coletados": api_meta.get("total_items"),
            "paginas": api_meta.get("total_pages"),
            "gravados": total_gravado,
        }

    fetch_and_upsert_task = PythonOperator(
        task_id='fetch_and_upsert_newcorban',
        python_callable=fetch_and_upsert_newcorban,
    )
