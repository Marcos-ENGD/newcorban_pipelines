import math
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import RealDictCursor, execute_batch


SOURCE_POSTGRES_CONN_ID = os.getenv("NEWCORBAN_SOURCE_CONN_ID", "246PGEsteiraQBCorban")
DEST_POSTGRES_CONN_ID = os.getenv("NEWCORBAN_DEST_CONN_ID", "Post_producao_246")

SOURCE_TABLE = os.getenv("NEWCORBAN_SOURCE_TABLE", "public.newcorban_propostas")
DEST_TABLE = os.getenv("NEWCORBAN_DEST_TABLE", "public.contratos2")
LOOKBACK_DAYS = int(os.getenv("NEWCORBAN_CONTRATOS2_LOOKBACK_DAYS", "7"))

SISTEMA_ORIGEM = "NEWCORBAN"

DEST_COLUMNS = [
    "af",
    "creationDate",
    "tipo",
    "dataEmissao",
    "fase",
    "cliente",
    "plataforma",
    "parceiro",
    "supervisor",
    "consultor",
    "orgao",
    "contratoRefin",
    "dataContratoRefin",
    "bancoRefin",
    "tabelaRefin",
    "prazoRefin",
    "valorEmprestimoRefin",
    "valorParcelaRefin",
    "valorRetorno",
    "numeroAde",
    "login",
    "cpf",
    "subStatus",
    "status",
    "dataStatus",
    "numeroContrato",
    "validoProducao",
    "dataProducao",
    "valorProducao",
    "updated_at",
    "dataAssinatura",
    "valorContrato",
    "validoProducaoBruto",
    "consultor_email",
    "contrato_id",
    "dataAverbacao",
    "taxa",
    "motivoStatus",
    "nomeStatus",
    "sistema_origem",
]


def is_nonempty(value):
    if value is None:
        return False
    if isinstance(value, float) and math.isnan(value):
        return False
    if isinstance(value, str) and not value.strip():
        return False
    return True


def to_str(value):
    if not is_nonempty(value):
        return None
    text = str(value).strip()
    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]
    return text or None


def to_upper(value):
    text = to_str(value)
    return text.upper() if text else None


def to_int(value):
    if not is_nonempty(value):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def to_decimal(value):
    if not is_nonempty(value):
        return None
    try:
        return Decimal(str(value).strip().replace(",", "."))
    except (InvalidOperation, ValueError, TypeError):
        return None


def to_datetime(value):
    if not is_nonempty(value):
        return None

    if isinstance(value, datetime):
        return value.replace(tzinfo=None)

    text = str(value).strip().replace("T", " ")
    for fmt in (
        "%Y-%m-%d %H:%M:%S.%f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d",
    ):
        try:
            return datetime.strptime(text, fmt)
        except Exception:
            pass
    return None


def normalize_key(value):
    text = to_str(value) or ""
    text = re.sub(r"\s+", " ", text.strip())
    return text.upper()


def build_af(newcorban_id):
    text = to_str(newcorban_id)
    if not text:
        return None
    return f"{text}NW"


def normalizar_tipo(product_name):
    produto = normalize_key(product_name)

    if produto in {"MARGEM LIVRE", "NOVO", "NOVOS"}:
        return "Novos"
    if produto in {"PORT COM REFIN", "PORT + REFIN", "PORTABILIDADE + REFINANCIAMENTO"}:
        return "Refin da Port"
    if produto in {"PORT COM REDUCAO", "PORT COM REDUCAO DE PARCELA", "PORTABILIDADE"}:
        return "Port com Reducao"
    if produto in {"REFIN", "REFINANCIAMENTO"}:
        return "Refinanciamento"
    if produto in {"CARTAO", "CARTAO BENEFICIO", "CARTAO CONSIGNADO"}:
        return "Cartao"

    return to_str(product_name)


def extrair_taxa(table_name):
    text = to_str(table_name)
    if not text:
        return None

    match = re.search(r"(\d{1,2}(?:[,.]\d{1,4})?)\s*%", text)
    if not match:
        return None

    return to_decimal(match.group(1))


def fase_integrada(row):
    status_id = to_int(row.get("status_id"))
    stage = normalize_key(row.get("stage"))

    return (
        status_id == 9115
        or stage in {"PAID", "COMPLETED", "INTEGRATED", "INTEGRADO"}
        or is_nonempty(row.get("payment_date"))
        or is_nonempty(row.get("completion_date"))
    )


def transformar_registro(row):
    af = build_af(row.get("id"))
    bank_proposal_number = to_str(row.get("bank_proposal_number"))

    if not af:
        return None

    if normalize_key(bank_proposal_number).startswith("QUA"):
        return None

    created_at = to_datetime(row.get("created_at"))
    registered_at = to_datetime(row.get("registered_at"))
    proposal_updated_at = to_datetime(row.get("proposal_updated_at"))
    formalization_date = to_datetime(row.get("formalization_date"))
    payment_date = to_datetime(row.get("payment_date"))
    endorsement_date = to_datetime(row.get("endorsement_date"))
    completion_date = to_datetime(row.get("completion_date"))
    cancellation_date = to_datetime(row.get("cancellation_date"))

    data_status = (
        proposal_updated_at
        or cancellation_date
        or completion_date
        or payment_date
        or endorsement_date
        or registered_at
        or created_at
    )
    data_producao = payment_date or completion_date or endorsement_date
    producao = 1 if fase_integrada(row) else 0

    tabela_refin = to_str(row.get("table_name")) or to_str(row.get("table_code"))

    return {
        "af": af,
        "creationDate": created_at or datetime.now(),
        "tipo": normalizar_tipo(row.get("product_name")),
        "dataEmissao": created_at,
        "fase": to_upper(row.get("status_name") or row.get("stage")),
        "cliente": to_str(row.get("customer_name")),
        "plataforma": to_str(row.get("plataforma")),
        "parceiro": to_str(row.get("plataforma")),
        "supervisor": to_str(row.get("supervisor")),
        "consultor": to_str(row.get("consultor") or row.get("seller_name")),
        "orgao": to_str(row.get("covenant_name")),
        "contratoRefin": bank_proposal_number,
        "dataContratoRefin": registered_at or formalization_date or created_at,
        "bancoRefin": to_str(row.get("bank_name")),
        "tabelaRefin": tabela_refin,
        "prazoRefin": to_int(row.get("term")),
        "valorEmprestimoRefin": to_decimal(row.get("financed_amount")),
        "valorParcelaRefin": to_decimal(row.get("installment_amount")),
        "valorRetorno": to_decimal(row.get("released_amount")),
        "numeroAde": bank_proposal_number,
        "login": None,
        "cpf": to_str(row.get("customer_cpf")),
        "subStatus": to_str(row.get("substatus")),
        "status": to_str(row.get("bank_status") or row.get("status_name")),
        "dataStatus": data_status,
        "numeroContrato": bank_proposal_number,
        "validoProducao": producao,
        "dataProducao": data_producao,
        "valorProducao": to_decimal(row.get("released_amount")) if producao else None,
        "updated_at": datetime.now(),
        "dataAssinatura": formalization_date,
        "valorContrato": to_decimal(row.get("financed_amount")),
        "validoProducaoBruto": producao,
        "consultor_email": None,
        "contrato_id": to_str(row.get("id")),
        "dataAverbacao": endorsement_date,
        "taxa": extrair_taxa(tabela_refin),
        "motivoStatus": to_str(row.get("substatus")),
        "nomeStatus": to_str(row.get("status_name")),
        "sistema_origem": SISTEMA_ORIGEM,
    }


def buscar_propostas_newcorban():
    hook = PostgresHook(postgres_conn_id=SOURCE_POSTGRES_CONN_ID)
    conn = hook.get_conn()

    sql = f"""
        SELECT
            id,
            is_duplicate,
            stage,
            customer_id,
            customer_name,
            customer_cpf,
            bank_id,
            bank_name,
            product_id,
            product_name,
            covenant_id,
            covenant_name,
            status_id,
            status_name,
            substatus,
            term,
            released_amount,
            financed_amount,
            installment_amount,
            seller_id,
            seller_name,
            typist_id,
            typist_name,
            bank_proposal_number,
            bank_status,
            created_at,
            registered_at,
            proposal_updated_at,
            formalization_date,
            payment_date,
            endorsement_date,
            cancellation_date,
            completion_date,
            synced_at,
            plataforma_id,
            plataforma,
            supervisor_id,
            supervisor,
            consultor_id,
            consultor,
            table_code,
            table_name
        FROM {SOURCE_TABLE}
        WHERE COALESCE(bank_proposal_number::text, '') NOT ILIKE 'QUA%%'
          AND synced_at >= NOW() - (%s * INTERVAL '1 day')
    """

    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(sql, (LOOKBACK_DAYS,))
        rows = cur.fetchall()

    conn.close()
    print(f"[origem] NewCorban encontrados: {len(rows)}")
    return [dict(row) for row in rows]


def get_existing_dest_columns(conn):
    schema, table = "public", DEST_TABLE
    if "." in DEST_TABLE:
        schema, table = DEST_TABLE.split(".", 1)

    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = %s
              AND table_name = %s
            """,
            (schema, table),
        )
        return {row[0] for row in cur.fetchall()}


def qident(name):
    return '"' + name.replace('"', '""') + '"'


def build_upsert_sql(columns):
    insert_cols = ",\n            ".join(qident(col) for col in columns)
    values_cols = ",\n            ".join(f"%({col})s" for col in columns)
    update_cols = [col for col in columns if col not in {"af", "creationDate"}]
    update_set = ",\n            ".join(
        f"{qident(col)} = EXCLUDED.{qident(col)}" for col in update_cols
    )

    return f"""
        INSERT INTO {DEST_TABLE} (
            {insert_cols}
        )
        VALUES (
            {values_cols}
        )
        ON CONFLICT (af) DO UPDATE SET
            {update_set}
    """


def upsert_contratos2(objetos, batch_size=1000):
    if not objetos:
        print("[destino] Nenhum registro para inserir/atualizar.")
        return 0

    hook = PostgresHook(postgres_conn_id=DEST_POSTGRES_CONN_ID)
    conn = hook.get_conn()
    conn.autocommit = False

    try:
        existing_columns = get_existing_dest_columns(conn)
        columns = [col for col in DEST_COLUMNS if col in existing_columns]
        missing = [col for col in DEST_COLUMNS if col not in existing_columns]

        if "af" not in columns:
            raise ValueError(f"Coluna af nao encontrada em {DEST_TABLE}.")

        if missing:
            print(f"[destino] Colunas ignoradas por nao existirem em {DEST_TABLE}: {missing}")

        payload = [{col: obj.get(col) for col in columns} for obj in objetos]
        sql = build_upsert_sql(columns)

        with conn.cursor() as cur:
            execute_batch(cur, sql, payload, page_size=batch_size)

        conn.commit()
        print(f"[destino] Upsert em {DEST_TABLE} concluido: {len(payload)} registros.")
        return len(payload)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def task_newcorban_para_contratos2():
    origem = buscar_propostas_newcorban()
    objetos = []

    for row in origem:
        try:
            obj = transformar_registro(row)
            if obj and obj.get("af"):
                objetos.append(obj)
        except Exception as exc:
            print(f"[transform] Erro no id={row.get('id')}: {exc}")

    print(f"[transform] Registros prontos para contratos2: {len(objetos)}")
    return upsert_contratos2(objetos)


with DAG(
    dag_id="etl_newcorban_para_contratos2",
    start_date=datetime(2026, 7, 10),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["newcorban", "contratos2", "producao"],
) as dag:
    rodar_etl = PythonOperator(
        task_id="rodar_etl_newcorban_para_contratos2",
        python_callable=task_newcorban_para_contratos2,
    )
