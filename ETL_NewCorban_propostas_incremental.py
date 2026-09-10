from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from dotenv import load_dotenv
import os

load_dotenv()
from datetime import datetime, timedelta
import requests
import time
import json
from psycopg2.extras import Json, execute_values


DAG_ID = "ETL_propostas_new_incremental"

POSTGRES_CONN_ID = "246PGEsteiraQBCorban"
TABLES_CONN_ID = "20PorcheNEW"

BASE_URL = "https://developers.newcorban.com.br/v1/proposals"
PER_PAGE = 200

DEFAULT_UPDATED_SINCE = "2026-06-01T00:00:00-03:00"


def get_token():
    token = os.getenv("NEWCORBAN_API")

    if not token:
        raise Exception("Token NEWCORBAN_API não encontrado no .env")

    return token

def get_state(pg):
    sql = """
        SELECT updated_since, last_cursor
        FROM newcorban_sync_state
        WHERE nome = 'proposals'
    """

    row = pg.get_first(sql)

    if not row:
        return DEFAULT_UPDATED_SINCE, None

    updated_since = row[0]
    if hasattr(updated_since, "isoformat"):
        updated_since = updated_since.isoformat()

    return updated_since, row[1]


def save_state(pg, updated_since, cursor):
    sql = """
        INSERT INTO newcorban_sync_state (
            nome,
            updated_since,
            last_cursor,
            last_run_at
        )
        VALUES (
            'proposals',
            %s,
            %s,
            NOW()
        )
        ON CONFLICT (nome)
        DO UPDATE SET
            updated_since = EXCLUDED.updated_since,
            last_cursor = EXCLUDED.last_cursor,
            last_run_at = NOW()
    """

    pg.run(sql, parameters=(updated_since, cursor))


def ensure_assignment_columns(pg):
    sql = """
        ALTER TABLE newcorban_propostas
            ADD COLUMN IF NOT EXISTS plataforma_id BIGINT,
            ADD COLUMN IF NOT EXISTS plataforma TEXT,
            ADD COLUMN IF NOT EXISTS supervisor_id BIGINT,
            ADD COLUMN IF NOT EXISTS supervisor TEXT,
            ADD COLUMN IF NOT EXISTS consultor_id BIGINT,
            ADD COLUMN IF NOT EXISTS consultor TEXT
    """

    pg.run(sql)


def backfill_assignment_columns(pg):
    sql = """
        UPDATE newcorban_propostas
        SET
            plataforma_id = NULLIF(raw #>> '{assignment,franchise,id}', '')::BIGINT,
            plataforma = raw #>> '{assignment,franchise,name}',
            supervisor_id = NULLIF(raw #>> '{assignment,team,id}', '')::BIGINT,
            supervisor = raw #>> '{assignment,team,name}',
            consultor_id = NULLIF(raw #>> '{assignment,seller,id}', '')::BIGINT,
            consultor = raw #>> '{assignment,seller,name}'
        WHERE raw IS NOT NULL
          AND raw #> '{assignment}' IS NOT NULL
          AND (
              plataforma_id IS NULL
              OR plataforma IS NULL
              OR supervisor_id IS NULL
              OR supervisor IS NULL
              OR consultor_id IS NULL
              OR consultor IS NULL
          )
    """

    pg.run(sql)


def request_api(token, params):
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    while True:
        response = requests.get(
            BASE_URL,
            headers=headers,
            params=params,
            timeout=60,
        )

        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", "30"))
            print(f"[RATE LIMIT] Aguardando {retry_after}s...")
            time.sleep(retry_after)
            continue

        if response.status_code >= 500:
            print(f"[ERRO {response.status_code}] Aguardando 20s e tentando novamente...")
            time.sleep(20)
            continue

        if response.status_code in (401, 403):
            raise Exception(f"Erro de autenticação/permissão: {response.status_code} - {response.text}")

        if response.status_code == 422:
            raise Exception(f"Erro de validação: {response.text}")

        response.raise_for_status()
        return response.json()


def parse_substatus(value):
    if value is None:
        return None

    if isinstance(value, str):
        return value

    return json.dumps(value, ensure_ascii=False)


def parse_api_datetime(value):
    if not value:
        return None

    if isinstance(value, datetime):
        return value

    text = str(value).strip()
    if not text:
        return None

    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")


def normalize_table_code(value):
    if value is None:
        return None

    text = str(value).strip()
    return text or None


def buscar_table_names(pg_tables, table_codes):
    codes = sorted({code for code in (normalize_table_code(c) for c in table_codes) if code})
    if not codes:
        return {}

    sql = """
        SELECT code::TEXT, name
        FROM tables
        WHERE code::TEXT = ANY(%s)
    """

    rows = pg_tables.get_records(sql, parameters=(codes,))
    return {normalize_table_code(code): name for code, name in rows}


def montar_linha(item, table_names=None):
    customer = item.get("customer") or {}
    proposal = item.get("proposal") or {}
    payment = item.get("payment") or {}
    assignment = item.get("assignment") or {}
    bank_reference = item.get("bank_reference") or {}
    dates = item.get("dates") or {}

    bank = proposal.get("bank") or {}
    product = proposal.get("product") or {}
    covenant = proposal.get("covenant") or {}
    status = proposal.get("status") or {}

    seller = assignment.get("seller") or {}
    typist = assignment.get("typist") or {}
    team = assignment.get("team") or {}
    franchise = assignment.get("franchise") or {}

    table_code = normalize_table_code(proposal.get("table_code"))
    table_name = (table_names or {}).get(table_code, proposal.get("table_name"))

    return (
        item.get("id"),
        item.get("is_duplicate"),
        item.get("stage"),

        customer.get("id"),
        customer.get("name"),
        customer.get("cpf"),

        bank.get("id"),
        bank.get("name"),
        product.get("id"),
        product.get("name"),
        covenant.get("id"),
        covenant.get("name"),

        status.get("id"),
        status.get("name"),
        parse_substatus(proposal.get("substatus")),

        proposal.get("term"),
        table_code,
        table_name,
        proposal.get("released_amount"),
        proposal.get("financed_amount"),
        proposal.get("installment_amount"),

        payment.get("type"),
        payment.get("bank_code"),
        payment.get("account"),
        payment.get("account_digit"),
        payment.get("branch"),
        payment.get("branch_digit"),
        payment.get("pix"),

        franchise.get("id"),
        franchise.get("name"),
        team.get("id"),
        team.get("name"),
        seller.get("id"),
        seller.get("name"),

        seller.get("id"),
        seller.get("name"),
        typist.get("id"),
        typist.get("name"),

        bank_reference.get("proposal_number"),
        bank_reference.get("bank_status"),

        dates.get("created_at"),
        dates.get("registered_at"),
        dates.get("updated_at"),
        dates.get("formalization_date"),
        dates.get("payment_date"),
        dates.get("endorsement_date"),
        dates.get("cancellation_date"),
        dates.get("completion_date"),

        Json(item),
    )


def upsert_propostas_lote(pg, linhas):
    if not linhas:
        return

    sql = """
        INSERT INTO newcorban_propostas (
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
            table_code,
            table_name,
            released_amount,
            financed_amount,
            installment_amount,

            payment_type,
            payment_bank_code,
            payment_account,
            payment_account_digit,
            payment_branch,
            payment_branch_digit,
            payment_pix,

            plataforma_id,
            plataforma,
            supervisor_id,
            supervisor,
            consultor_id,
            consultor,

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

            raw,
            synced_at
        )
        VALUES %s
        ON CONFLICT (id)
        DO UPDATE SET
            is_duplicate = EXCLUDED.is_duplicate,
            stage = EXCLUDED.stage,

            customer_id = EXCLUDED.customer_id,
            customer_name = EXCLUDED.customer_name,
            customer_cpf = EXCLUDED.customer_cpf,

            bank_id = EXCLUDED.bank_id,
            bank_name = EXCLUDED.bank_name,
            product_id = EXCLUDED.product_id,
            product_name = EXCLUDED.product_name,
            covenant_id = EXCLUDED.covenant_id,
            covenant_name = EXCLUDED.covenant_name,

            status_id = EXCLUDED.status_id,
            status_name = EXCLUDED.status_name,
            substatus = EXCLUDED.substatus,

            term = EXCLUDED.term,
            table_code = EXCLUDED.table_code,
            table_name = EXCLUDED.table_name,
            released_amount = EXCLUDED.released_amount,
            financed_amount = EXCLUDED.financed_amount,
            installment_amount = EXCLUDED.installment_amount,

            payment_type = EXCLUDED.payment_type,
            payment_bank_code = EXCLUDED.payment_bank_code,
            payment_account = EXCLUDED.payment_account,
            payment_account_digit = EXCLUDED.payment_account_digit,
            payment_branch = EXCLUDED.payment_branch,
            payment_branch_digit = EXCLUDED.payment_branch_digit,
            payment_pix = EXCLUDED.payment_pix,

            plataforma_id = EXCLUDED.plataforma_id,
            plataforma = EXCLUDED.plataforma,
            supervisor_id = EXCLUDED.supervisor_id,
            supervisor = EXCLUDED.supervisor,
            consultor_id = EXCLUDED.consultor_id,
            consultor = EXCLUDED.consultor,

            seller_id = EXCLUDED.seller_id,
            seller_name = EXCLUDED.seller_name,
            typist_id = EXCLUDED.typist_id,
            typist_name = EXCLUDED.typist_name,

            bank_proposal_number = EXCLUDED.bank_proposal_number,
            bank_status = EXCLUDED.bank_status,

            created_at = EXCLUDED.created_at,
            registered_at = EXCLUDED.registered_at,
            proposal_updated_at = EXCLUDED.proposal_updated_at,
            formalization_date = EXCLUDED.formalization_date,
            payment_date = EXCLUDED.payment_date,
            endorsement_date = EXCLUDED.endorsement_date,
            cancellation_date = EXCLUDED.cancellation_date,
            completion_date = EXCLUDED.completion_date,

            raw = EXCLUDED.raw,
            synced_at = NOW()
    """

    conn = pg.get_conn()

    with conn.cursor() as cur:
        execute_values(
            cur,
            sql,
            linhas,
            template="""
            (
                %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,%s, %s,
                %s, %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s, %s, %s, %s, %s, %s,
                %s,
                NOW()
            )
            """,
            page_size=1000
        )

    conn.commit()


def sync_newcorban_propostas(**context):

    token = get_token()
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    pg_tables = PostgresHook(postgres_conn_id=TABLES_CONN_ID)
    ensure_assignment_columns(pg)
    backfill_assignment_columns(pg)

    updated_since, cursor = get_state(pg)

    print(f"[INÍCIO] updated_since={updated_since} cursor={cursor}")

    total = 0
    maior_updated_at = None

    while True:
        if cursor:
            params = {
                "cursor": cursor,
                "per_page": PER_PAGE,
            }
        else:
            params = {
                "updated_since": updated_since,
                "per_page": PER_PAGE,
            }

        payload = request_api(token, params)

        data = payload.get("data") or []
        meta = payload.get("meta") or {}
        next_cursor = meta.get("next_cursor")
        linhas_para_insert = []
        table_names = buscar_table_names(
            pg_tables,
            [((item.get("proposal") or {}).get("table_code")) for item in data],
        )

        print(f"[PÁGINA] registros={len(data)} next_cursor={next_cursor}")

        for item in data:
            linhas_para_insert.append(montar_linha(item, table_names))
            total += 1

            item_updated_at = (item.get("dates") or {}).get("updated_at")
            if item_updated_at:
                if maior_updated_at is None or item_updated_at > maior_updated_at:
                    maior_updated_at = item_updated_at

        print(f"[INSERT LOTE] Inserindo/atualizando {len(linhas_para_insert)} propostas em lote...")
        upsert_propostas_lote(pg, linhas_para_insert)
        print("[INSERT LOTE] Finalizado.")

        save_state(pg, updated_since, next_cursor)

        if not next_cursor:
            break

        cursor = next_cursor

    if maior_updated_at:
        dt = parse_api_datetime(maior_updated_at)

        novo_updated_since = (
            dt - timedelta(minutes=2)
        ).strftime("%Y-%m-%dT%H:%M:%S-03:00")

        save_state(pg, novo_updated_since, None)

        print(f"[CHECKPOINT] Novo updated_since={novo_updated_since}")
    else:
        save_state(pg, updated_since, None)

    print(f"[FIM] Total processado={total}")


default_args = {
    "owner": "qualiconsig",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2026, 6, 23),
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["newcorban", "propostas", "incremental"],
) as dag:

    task_sync = PythonOperator(
        task_id="sync_propostas_incremental",
        python_callable=sync_newcorban_propostas,
        provide_context=True,
    )
