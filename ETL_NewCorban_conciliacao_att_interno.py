from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.dates import days_ago
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import timedelta
from pathlib import Path
import os
import re
import time
import unicodedata
import json
import requests

import pandas as pd
from dotenv import load_dotenv


load_dotenv()
logger = LoggingMixin().log

POSTGRES_CONN_ID = "246PGEsteiraQBCorban"
DAG_ID = "conciliacao_newcorban_att_interno"

ATT_TABLE = "newcorban_att_interno"
FASES_TABLE = "newcorban_fases_interno"
PROPOSTAS_TABLE = "newcorban_propostas"
AUDITORIA_ATUALIZACOES_TABLE = "newcorban_conciliacao_att_interno_atualizacoes"

FASE_INTEGRADO = "Integrado"
FASE_AGUARDANDO_AVERBACAO = "Aguardando Averbação"
FASE_AGUARDANDO_RETORNO_CIP = "Aguardando Retorno CIP"
FASE_AGUARDA_AVERBACAO = "Aguarda Averbação"
FASE_CIP_RETORNADA = "CIP Retornada"
FASE_AGUARDANDO_SALDO = "Aguardando Saldo"
FASE_AGUARDANDO_SALDO_ID = 9148
FASE_REPROVA_POS_CIP = "Reprova Pos-CIP"
FASE_REPROVA_POS_CIP_ID = 9112
MOTIVO_REPROVA_POS_CIP = "Reprova Pos-CIP"
FASE_REPROVADA_CANCELADA = "Reprovada - Cancelada"
FASE_REPROVADA_CANCELADA_ID = 9117
FASE_INTEGRADO_ID = 9115
FASE_DIGITADO_STAND_BY = "Digitado Stand By"
FASE_MARGEM_NEGATIVA = "Margem Negativa – Saldo Pago"
FASE_AGUARDANDO_APROVACAO_SUPERVISOR = "Aguardando aprovação do Supervisor"
FASE_AGUARDANDO_APROVACAO_SUPERVISOR_ID = 78
FASE_ASSINATURA_REALIZADA = "Assinatura Realizada"
FASE_EM_ANALISE_ACOMPANHAMENTO = "Em análise - Acompanhamento"
FASE_EM_ANALISE_ACOMPANHAMENTO_ID = 9106
MOTIVO_AGUARDANDO_APROVACAO = "Aguardando Aprovação"
FASE_INCONSISTENCIA_CIP_RETORNADA = "Inconsist\u00eancia CIP retornada"
FASE_FRAUDE_DECTADA = "Fraude Dectada"
FASE_REAPRESENTACAO_PAGAMENTO = "Reapresenta\u00e7\u00e3o de Pagamento"
FASE_INCONSISTENCIA_POS_PAGAMENTO = "Inconsist\u00eancia P\u00f3s Pagamento"
FASE_TRATATIVA_COMERCIAL = "Tratativa Comercial"
FASE_TRATATIVA_FINALIZADA = "Tratativa Finalizada"
STATUS_AVERBADO_PENDENTE_ANUENCIA = "Averbado - Pendente de Anuencia"
MOTIVO_AGUARDANDO_AVERBACAO_PORT = "Ag. Averba\u00e7\u00e3o - Port (Dentro do prazo)"
MOTIVO_AGUARDANDO_SALDO_REFIN_PORT = "Aguardando Pagamento Portabilidade"
MOTIVO_PORT_AGUARDANDO_CIP = "Port Aguardando CIP"
MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO = "Port Paga / Aguardando Averba\u00e7\u00e3o"
MOTIVO_PORTABILIDADE_AVERBADA = "Portabilidade Averbada"
MOTIVO_PORTABILIDADE_FINALIZADA_CONTRATO = "Portabilidade já finalizada para o contrato informado"
MOTIVO_PORTABILIDADE_FINALIZADA = "Portabilidade Já Finalizada"
MOTIVOS_SUBSTATUS_PROTEGIDOS = {
    "Desist\u00eancia do Cliente",
    "Biometria N\u00e3o coletada",
    "Comparecer INSS",
    "Recupera\u00e7\u00e3o de Senha",
    "Dados Divergentes na Receita Federal",
    "Sem contato",
}
FASES_REPROVACAO_BLOQUEIO = (
    "Reprovada - Cancelada",
    "Cancelado",)

STATUS_REQUIRED_DATE_FIELD = {
    9113: "balance_return_date",
    9118: "cancellation_date",
    9115: "payment_date",
    9112: "cancellation_date",
    9117: "cancellation_date",
}

DEFAULT_OUTPUT_DIR = os.getenv("NEWCORBAN_CONCILIACAO_OUTPUT_DIR", "/home/qualiconsig")
OUTPUT_FILE_NAME = os.getenv(
    "NEWCORBAN_CONCILIACAO_OUTPUT_FILE",
    "conciliacao_newcorban_att_interno.xlsx",
)
FINANCIAL_PASSWORD = os.getenv("NEWCORBAN_FINANCIAL_PASSWORD")
NEWCORBAN_TOKEN = 'nc_live_PsS9B39OC4kk2UoPShOCiksMOM8C5QwNbsUJFleH'
API_TIMEOUT_SECONDS = int(os.getenv("NEWCORBAN_STATUS_API_TIMEOUT_SECONDS", "60"))
API_MIN_DELAY_SECONDS = 2.0
API_RATE_LIMIT_MIN_REMAINING = 10
API_RATE_LIMIT_MAX_SLEEP_SECONDS = 15.0

TIPO_MAP = {
    1: "Novos",
    2: "Refinanciamento",
    3: "Port com Reducao",
    4: "Port + Refin",
    5: "Refin da Port",
    6: "Novos",
    13: "Seguro",
    9: "Cartao",
}

PRODUTO_MAP = {
    "NOVOS": "NOVO",
    "PORT": "PORTABILIDADE",
    "PORT/NOVO": "PORT/NOVO",
    "PORT / NOVO": "PORT/NOVO",
    "REFIN": "REFINANCIAMENTO",
    "PORT COM REFIN": "PORTABILIDADE",
    "PORT COM REDUCAO": "PORTABILIDADE",
    "Novos": "NOVO",
    "NOVO": "NOVO",
    "Portabilidade": "PORTABILIDADE",
    "PORTABILIDADE": "PORTABILIDADE",
    "Port + Refin": "PORTABILIDADE",
    "Port com Refin": "PORTABILIDADE",
    "PORT + REFIN": "PORTABILIDADE",
    "Refin da Port": "PORTABILIDADE",
    "Port com Reducao": "PORTABILIDADE",
    "Port com Redução": "PORTABILIDADE",
    "REFIN DA PORT": "PORTABILIDADE",
    "Refinanciamento": "REFINANCIAMENTO",
    "REFINANCIAMENTO": "REFINANCIAMENTO",
    "Seguro": "NOVO",
    "SEGURO": "NOVO",
    "Cartao": "CARTAO",
    "CARTAO": "CARTAO",
    "Cartão": "CARTAO",
    "CARTÃO": "CARTAO",
}


def normalize_text(value) -> str:
    if value is None or pd.isna(value):
        return ""

    text = str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    text = re.sub(r"\s+", " ", text)
    return text.upper()


def normaliza_produto_depara(produto) -> str:
    if produto is None or pd.isna(produto):
        return ""

    produto_norm = normalize_text(produto)
    return PRODUTO_MAP.get(produto_norm, produto_norm)


def produto_compativel(produto_api: str, produto_depara: str) -> bool:
    produto_api = normaliza_produto_depara(produto_api)
    produto_depara = normaliza_produto_depara(produto_depara)

    if not produto_api or not produto_depara:
        return False

    if produto_depara == "PORT/NOVO":
        return produto_api in {"PORTABILIDADE", "NOVO"}

    if produto_depara == "NOVO":
        return produto_api == "NOVO"

    if produto_depara == "PORTABILIDADE":
        return produto_api == "PORTABILIDADE"

    if produto_depara == "REFINANCIAMENTO":
        return produto_api == "REFINANCIAMENTO"

    if produto_depara == "SEGURO":
        return produto_api == "SEGURO"

    if produto_depara == "CARTAO":
        return produto_api == "CARTAO"

    return produto_api == produto_depara


def normalize_key(value) -> str:
    text = normalize_text(value)
    return re.sub(r"[^A-Z0-9]+", "", text)


def normalize_af(value):
    if isinstance(value, pd.Series):
        value = value.iloc[0] if not value.empty else None

    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    if not text:
        return None

    if re.fullmatch(r"\d+\.0", text):
        text = text[:-2]

    return text


def date_only(value):
    if value is None or pd.isna(value):
        return None

    dt = pd.to_datetime(value, errors="coerce")
    if pd.isna(dt):
        return None

    return dt.strftime("%Y-%m-%d")


def clean_payload_value(value):
    if value is None or pd.isna(value):
        return None

    if isinstance(value, float) and value.is_integer():
        return int(value)

    return value


def clean_text_value(value):
    if value is None or pd.isna(value):
        return None

    text = str(value).strip()
    return text or None


def parse_bool(value) -> bool:
    if isinstance(value, bool):
        return value

    if value is None:
        return False

    return str(value).strip().lower() in {"1", "true", "t", "sim", "s", "yes", "y"}


def parse_int_or_none(value):
    try:
        if value is None:
            return None
        return int(float(value))
    except (TypeError, ValueError):
        return None


def get_rate_limit_info(response) -> dict:
    limit = parse_int_or_none(response.headers.get("X-Ratelimit-Limit"))
    remaining = parse_int_or_none(response.headers.get("X-Ratelimit-Remaining"))
    reset = parse_int_or_none(response.headers.get("X-Ratelimit-Reset"))

    sleep_seconds = API_MIN_DELAY_SECONDS
    if response.status_code == 429 or (remaining is not None and remaining <= API_RATE_LIMIT_MIN_REMAINING):
        if reset:
            sleep_seconds = max(reset - time.time() + 1, API_MIN_DELAY_SECONDS)
        else:
            sleep_seconds = API_RATE_LIMIT_MAX_SLEEP_SECONDS

    sleep_seconds = min(max(sleep_seconds, 0), API_RATE_LIMIT_MAX_SLEEP_SECONDS)

    return {
        "api_ratelimit_limit": limit,
        "api_ratelimit_remaining": remaining,
        "api_ratelimit_reset": reset,
        "api_sleep_seconds": sleep_seconds,
    }


def build_update_payload(row: pd.Series):
    if row.get("resultado") != "falta_atualizar":
        return None

    proposta_id = clean_payload_value(row.get("proposta_id"))
    status_id = clean_payload_value(row.get("nw_fase_id"))

    if proposta_id is None or status_id is None:
        return None

    try:
        status_id = int(status_id)
    except (TypeError, ValueError):
        return None

    motivo = clean_text_value(row.get("depara_motivo"))
    if normalize_text(motivo) == normalize_text(MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO):
        motivo = MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO
    note = clean_text_value(row.get("nw_fase")) or "Ajuste de status conforme retorno do banco."
    if motivo:
        note = f"{note} Motivo: {motivo}"

    payload = {
        "status_id": status_id,
        "note": note,
    }
    if motivo and not row.get("motivo_substatus_protegido"):
        payload["substatus"] = motivo
    if FINANCIAL_PASSWORD:
        payload["financial_password"] = FINANCIAL_PASSWORD

    required_date_field = STATUS_REQUIRED_DATE_FIELD.get(status_id)
    if required_date_field:
        date_source = row.get("data_retorno_cip") if required_date_field == "balance_return_date" else row.get("dataStatus")
        date_value = date_only(date_source) or date_only(row.get("dataStatus"))
        if not date_value:
            return None

        payload["dates"] = {required_date_field: date_value}

    return payload


def build_update_payload_error(row: pd.Series):
    if row.get("resultado") != "falta_atualizar":
        return None

    proposta_id = clean_payload_value(row.get("proposta_id"))
    status_id = clean_payload_value(row.get("nw_fase_id"))

    if proposta_id is None:
        return "proposta_id ausente"

    if status_id is None:
        return "nw_fase_id ausente"

    try:
        status_id = int(status_id)
    except (TypeError, ValueError):
        return "nw_fase_id invalido"

    required_date_field = STATUS_REQUIRED_DATE_FIELD.get(status_id)
    if required_date_field:
        date_source = row.get("data_retorno_cip") if required_date_field == "balance_return_date" else row.get("dataStatus")
        date_value = date_only(date_source) or date_only(row.get("dataStatus"))
        if not date_value:
            return f"data obrigatoria ausente: {required_date_field}"

    return None


def precisa_limpar_formalizador(row: pd.Series) -> bool:
    if row.get("resultado") != "falta_atualizar":
        return False

    crm_status_norm = row.get("crm_status_norm")
    nw_fase_norm = row.get("nw_fase_norm")
    destinos_averbacao = {
        normalize_key(FASE_AGUARDANDO_AVERBACAO),
        normalize_key(FASE_AGUARDA_AVERBACAO),
    }

    return (
        crm_status_norm == normalize_key(FASE_AGUARDANDO_RETORNO_CIP)
        and nw_fase_norm == normalize_key(FASE_CIP_RETORNADA)
    ) or (
        crm_status_norm == normalize_key(FASE_CIP_RETORNADA)
        and nw_fase_norm in destinos_averbacao
    )


def build_clear_formalizer_payload(row: pd.Series):
    if not precisa_limpar_formalizador(row):
        return None

    proposta_id = clean_payload_value(row.get("proposta_id"))
    if proposta_id is None:
        return None

    return {
        "assignment": {
            "formalizer_id": "",
        }
    }


def build_substatus_payload(row: pd.Series):
    if row.get("resultado") != "falta_atualizar":
        return None
    if row.get("motivo_substatus_protegido"):
        return None

    proposta_id = clean_payload_value(row.get("proposta_id"))
    motivo = clean_text_value(row.get("depara_motivo"))
    if normalize_text(motivo) == normalize_text(MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO):
        motivo = MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO
    if proposta_id is None or not motivo:
        return None

    return {
        "proposal": {
            "substatus": motivo,
        }
    }


def call_status_api(row: pd.Series, token: str) -> dict:
    payload = row.get("update_payload")
    endpoint = row.get("api_endpoint")

    if not isinstance(payload, dict) or not endpoint:
        return {
            "api_executado": False,
            "api_status_code": None,
            "api_sucesso": None,
            "api_response_json": None,
            "api_response_text": None,
            "api_erro": "payload ou endpoint ausente",
            "api_ratelimit_limit": None,
            "api_ratelimit_remaining": None,
            "api_ratelimit_reset": None,
            "api_sleep_seconds": None,
        }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.put(
            endpoint,
            headers=headers,
            json=payload,
            timeout=API_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return {
            "api_executado": True,
            "api_status_code": None,
            "api_sucesso": False,
            "api_response_json": None,
            "api_response_text": None,
            "api_erro": str(exc),
            "api_ratelimit_limit": None,
            "api_ratelimit_remaining": None,
            "api_ratelimit_reset": None,
            "api_sleep_seconds": API_MIN_DELAY_SECONDS,
        }

    response_json = None
    response_text = None
    try:
        response_json = response.json()
    except Exception:
        response_text = response.text

    result = {
        "api_executado": True,
        "api_status_code": response.status_code,
        "api_sucesso": 200 <= response.status_code < 300,
        "api_response_json": json.dumps(response_json, ensure_ascii=False, default=str)
        if response_json is not None
        else None,
        "api_response_text": response_text,
        "api_erro": None if 200 <= response.status_code < 300 else (response_text or json.dumps(response_json, ensure_ascii=False, default=str)),
    }
    
    result.update(get_rate_limit_info(response))
    return result


def call_clear_formalizer_api(row: pd.Series, token: str) -> dict:
    payload = row.get("formalizer_payload")
    endpoint = row.get("formalizer_endpoint")

    if not isinstance(payload, dict) or not endpoint:
        return {
            "formalizer_api_executado": False,
            "formalizer_status_code": None,
            "formalizer_sucesso": None,
            "formalizer_response_json": None,
            "formalizer_response_text": None,
            "formalizer_erro": "payload ou endpoint ausente",
            "formalizer_ratelimit_remaining": None,
            "formalizer_sleep_seconds": None,
        }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.put(
            endpoint,
            headers=headers,
            json=payload,
            timeout=API_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return {
            "formalizer_api_executado": True,
            "formalizer_status_code": None,
            "formalizer_sucesso": False,
            "formalizer_response_json": None,
            "formalizer_response_text": None,
            "formalizer_erro": str(exc),
            "formalizer_ratelimit_remaining": None,
            "formalizer_sleep_seconds": API_MIN_DELAY_SECONDS,
        }

    response_json = None
    response_text = None
    try:
        response_json = response.json()
    except Exception:
        response_text = response.text

    rate_limit_info = get_rate_limit_info(response)
    return {
        "formalizer_api_executado": True,
        "formalizer_status_code": response.status_code,
        "formalizer_sucesso": 200 <= response.status_code < 300,
        "formalizer_response_json": json.dumps(response_json, ensure_ascii=False, default=str)
        if response_json is not None
        else None,
        "formalizer_response_text": response_text,
        "formalizer_erro": None if 200 <= response.status_code < 300 else (response_text or json.dumps(response_json, ensure_ascii=False, default=str)),
        "formalizer_ratelimit_remaining": rate_limit_info.get("api_ratelimit_remaining"),
        "formalizer_sleep_seconds": rate_limit_info.get("api_sleep_seconds"),
    }


def call_substatus_api(row: pd.Series, token: str) -> dict:
    payload = row.get("substatus_payload")
    endpoint = row.get("substatus_endpoint")

    if not isinstance(payload, dict) or not endpoint:
        return {
            "substatus_api_executado": False,
            "substatus_status_code": None,
            "substatus_sucesso": None,
            "substatus_response_json": None,
            "substatus_response_text": None,
            "substatus_erro": "payload ou endpoint ausente",
            "substatus_ratelimit_remaining": None,
            "substatus_sleep_seconds": None,
        }

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {token}",
    }

    try:
        response = requests.put(
            endpoint,
            headers=headers,
            json=payload,
            timeout=API_TIMEOUT_SECONDS,
        )
    except Exception as exc:
        return {
            "substatus_api_executado": True,
            "substatus_status_code": None,
            "substatus_sucesso": False,
            "substatus_response_json": None,
            "substatus_response_text": None,
            "substatus_erro": str(exc),
            "substatus_ratelimit_remaining": None,
            "substatus_sleep_seconds": API_MIN_DELAY_SECONDS,
        }

    response_json = None
    response_text = None
    try:
        response_json = response.json()
    except Exception:
        response_text = response.text

    rate_limit_info = get_rate_limit_info(response)
    return {
        "substatus_api_executado": True,
        "substatus_status_code": response.status_code,
        "substatus_sucesso": 200 <= response.status_code < 300,
        "substatus_response_json": json.dumps(response_json, ensure_ascii=False, default=str)
        if response_json is not None
        else None,
        "substatus_response_text": response_text,
        "substatus_erro": None if 200 <= response.status_code < 300 else (response_text or json.dumps(response_json, ensure_ascii=False, default=str)),
        "substatus_ratelimit_remaining": rate_limit_info.get("api_ratelimit_remaining"),
        "substatus_sleep_seconds": rate_limit_info.get("api_sleep_seconds"),
    }


def produto_from_tipo(tipo_id):
    try:
        tipo_id = int(tipo_id)
    except (TypeError, ValueError):
        return None

    tipo_nome = TIPO_MAP.get(tipo_id)
    return normaliza_produto_depara(tipo_nome)


def get_series(df: pd.DataFrame, column_name: str) -> pd.Series:
    value = df[column_name]
    if isinstance(value, pd.DataFrame):
        logger.warning(
            "[CONCILIACAO] Coluna duplicada detectada: %s. Usando a primeira ocorrencia.",
            column_name,
        )
        return value.iloc[:, 0]
    return value


def first_notna(values):
    for value in values:
        if value is not None and not pd.isna(value):
            return value
    return None


def lookup_nw_fase_id(fases_df: pd.DataFrame, fase_nome: str, produto_norm: str = None):
    fase_norm = normalize_key(fase_nome)
    candidatos = fases_df[
        (get_series(fases_df, "nw_fase").apply(normalize_key) == fase_norm)
        & (get_series(fases_df, "nw_fase_id").notna())
    ].copy()

    if produto_norm:
        candidatos_produto = candidatos[
            get_series(candidatos, "produto_depara_norm").map(
                lambda produto: produto_compativel(produto_norm, produto)
            )
        ]
        if not candidatos_produto.empty:
            candidatos = candidatos_produto

    return first_notna(get_series(candidatos, "nw_fase_id").tolist())


def aplicar_fase_forcada(
    df: pd.DataFrame,
    mask,
    fase_nome: str,
    fase_id,
    regra: str,
    observacao: str,
    motivo=None,
) -> None:
    if not mask.any():
        return

    df.loc[mask, "nw_fase"] = fase_nome
    df.loc[mask, "nw_fase_id"] = fase_id
    df.loc[mask, "nw_fase_norm"] = normalize_key(fase_nome)
    df.loc[mask, "regra_conciliacao"] = regra
    df.loc[mask, "observacao_regra"] = observacao
    df.loc[mask & (df["depara_status"] == "sem_depara"), "depara_status"] = regra
    if motivo is not None:
        df.loc[mask, "depara_motivo"] = motivo


def load_dataframes(pg: PostgresHook):
    sql_att = f"""
        SELECT
            af,
            "numeroAde" AS numero_ade,
            fase,
            fase_id,
            tipo_id,
            nota_status,
            "numeroContrato" AS numero_contrato,
            "dataStatus",
            "dataRetornoCip" AS data_retorno_cip,
            consultor,
            plataforma,
            cpf,
            beneficio
        FROM {ATT_TABLE}
        WHERE COALESCE(NULLIF(TRIM(af), ''), NULLIF(TRIM("numeroAde"), '')) IS NOT NULL
"""


    sql_fases = f"""
        SELECT
            status,
            status_id,
            motivo,
            produto,
            nw_fase,
            nw_fase_id
        FROM {FASES_TABLE}
        WHERE status_id IS NOT NULL
    """

    sql_propostas = f"""
        SELECT
            id AS proposta_id,
            bank_proposal_number,
            status_id AS crm_status_id,
            status_name AS crm_status_name,
            substatus AS crm_substatus,
            customer_name,
            customer_cpf,
            product_name,
            bank_status,
            proposal_updated_at,
            synced_at
        FROM {PROPOSTAS_TABLE}
        WHERE bank_proposal_number IS NOT NULL and plataforma not ilike 'PARCEIRO%' 
    """

    conn = pg.get_conn()
    try:
        return (
            pd.read_sql(sql_att, conn),
            pd.read_sql(sql_fases, conn),
            pd.read_sql(sql_propostas, conn),
        )
    finally:
        conn.close()


def escolher_depara(row: pd.Series, fases_df: pd.DataFrame) -> pd.Series:
    fase = row.get("fase_id")
    produto_norm = row.get("produto_norm")
    nota_norm = row.get("nota_status_norm")

    if pd.isna(fase) or not produto_norm:
        return pd.Series({
            "depara_status": "sem_depara",
            "depara_chave": None,
            "depara_status_origem": None,
            "depara_motivo": None,
            "depara_produto": None,
            "nw_fase": None,
            "nw_fase_id": None,
            "depara_qtd_candidatos": 0,
        })

    candidatos_status = fases_df[fases_df["status_id_num"] == fase].copy()
    candidatos = candidatos_status[
        candidatos_status["produto_depara_norm"].map(lambda produto: produto_compativel(produto_norm, produto))
    ].copy()

    if candidatos.empty:
        return pd.Series({
            "depara_status": "sem_depara",
            "depara_chave": fase,
            "depara_status_origem": None,
            "depara_motivo": None,
            "depara_produto": None,
            "nw_fase": None,
            "nw_fase_id": None,
            "depara_qtd_candidatos": 0,
        })

    if len(candidatos) == 1:
        escolhido = candidatos.iloc[0]
        depara_status = (
            "ok_motivo"
            if escolhido.get("motivo_norm") != ""
            else "ok_sem_motivo"
        )
    else:
        motivo_especifico = candidatos[candidatos["motivo_norm"] != ""]
        motivo_match = motivo_especifico[motivo_especifico["motivo_norm"] == nota_norm]
        if not motivo_match.empty:
            escolhido = motivo_match.iloc[0]
            depara_status = "ok_motivo"
        else:
            sem_motivo = candidatos[candidatos["motivo_norm"] == ""]
            if not sem_motivo.empty:
                escolhido = sem_motivo.iloc[0]
                depara_status = "ok_sem_motivo"
            else:
                escolhido = candidatos.iloc[0]
                depara_status = "motivo_nao_encontrado"

    return pd.Series({
        "depara_status": depara_status,
        "depara_chave": fase,
        "depara_status_origem": escolhido.get("status"),
        "depara_motivo": escolhido.get("motivo"),
        "depara_produto": escolhido.get("produto"),
        "nw_fase": escolhido.get("nw_fase"),
        "nw_fase_id": escolhido.get("nw_fase_id"),
        "depara_qtd_candidatos": len(candidatos),
    })


def build_conciliacao(
    df_att: pd.DataFrame,
    df_fases: pd.DataFrame,
    df_prop: pd.DataFrame,
    executar_api: bool = False,
    token: str = None,
) -> pd.DataFrame:
    df_att = df_att.copy()
    df_fases = df_fases.copy()
    df_prop = df_prop.copy()

    df_att["af_match"] = get_series(df_att, "af").apply(normalize_af)
    df_att["af_match"] = df_att["af_match"].fillna(get_series(df_att, "numero_ade").apply(normalize_af))
    df_att["fase"] = pd.to_numeric(get_series(df_att, "fase"), errors="coerce").astype("Int64")
    df_att["fase_id"] = pd.to_numeric(get_series(df_att, "fase_id"), errors="coerce").astype("Int64")
    df_att["tipo_id"] = pd.to_numeric(get_series(df_att, "tipo_id"), errors="coerce").astype("Int64")
    df_att["tipo_nome"] = df_att["tipo_id"].map(lambda x: TIPO_MAP.get(int(x)) if pd.notna(x) else None)
    df_att["produto_norm"] = df_att["tipo_id"].map(produto_from_tipo)
    df_att["nota_status_norm"] = get_series(df_att, "nota_status").apply(normalize_text)
    df_att["numero_contrato_match"] = get_series(df_att, "numero_contrato").apply(normalize_af)

    df_fases["status_id_num"] = pd.to_numeric(get_series(df_fases, "status_id"), errors="coerce").astype("Int64")
    df_fases["produto_depara_norm"] = get_series(df_fases, "produto").map(normaliza_produto_depara)
    df_fases["motivo_norm"] = get_series(df_fases, "motivo").apply(normalize_text)

    depara = df_att.apply(lambda row: escolher_depara(row, df_fases), axis=1)
    df = pd.concat([df_att, depara], axis=1)

    df_prop["af_match"] = get_series(df_prop, "bank_proposal_number").apply(normalize_af)
    df_prop = (
        df_prop.sort_values(["proposal_updated_at", "synced_at"], ascending=[False, False])
        .drop_duplicates(subset=["af_match"], keep="first")
    )

    df = df.merge(
        df_prop,
        on="af_match",
        how="left",
        suffixes=("_att", "_crm"),
    )

    df["nw_fase_norm"] = df["nw_fase"].apply(normalize_key)
    df["nw_fase_norm_depara_original"] = df["nw_fase_norm"]
    df["depara_motivo_original"] = df["depara_motivo"]
    df["depara_status_original"] = df["depara_status"]
    df["crm_status_norm"] = df["crm_status_name"].apply(normalize_key)
    df["crm_substatus_norm"] = df["crm_substatus"].apply(normalize_text)
    df["depara_status_origem_norm"] = df["depara_status_origem"].apply(normalize_key)
    df["regra_conciliacao"] = "normal"
    df["observacao_regra"] = None

    aguardando_averbacao_id = first_notna([
        lookup_nw_fase_id(df_fases, FASE_AGUARDANDO_AVERBACAO, "PORTABILIDADE"),
        lookup_nw_fase_id(df_fases, FASE_AGUARDA_AVERBACAO, "PORTABILIDADE"),
    ])
    digitado_stand_by_id = lookup_nw_fase_id(df_fases, FASE_DIGITADO_STAND_BY)

    port_refin_retorno_cip = df[
        (df["tipo_nome"] == "Port + Refin")
        & (df["numero_contrato_match"].notna())
        & (df["nw_fase_norm"] == normalize_key(FASE_AGUARDANDO_RETORNO_CIP))
    ]
    contratos_forcar_saldo = set(port_refin_retorno_cip["numero_contrato_match"].dropna())

    if contratos_forcar_saldo:
        mask_refin_saldo = (
            (df["tipo_nome"] == "Refin da Port")
            & (df["numero_contrato_match"].isin(contratos_forcar_saldo))
        )
        df.loc[mask_refin_saldo, "nw_fase"] = FASE_AGUARDANDO_SALDO
        df.loc[mask_refin_saldo, "nw_fase_id"] = FASE_AGUARDANDO_SALDO_ID
        df.loc[mask_refin_saldo, "nw_fase_norm"] = normalize_key(FASE_AGUARDANDO_SALDO)
        df.loc[mask_refin_saldo, "depara_motivo"] = MOTIVO_PORT_AGUARDANDO_CIP
        df.loc[mask_refin_saldo, "regra_conciliacao"] = "refin_saldo_port_aguardando_cip"
        df.loc[
            mask_refin_saldo & (df["depara_status"] == "sem_depara"),
            "depara_status",
        ] = "refin_saldo_port_aguardando_cip"
        df.loc[
            mask_refin_saldo,
            "observacao_regra",
        ] = "Refin da Port forçado para Aguardando Saldo porque Port + Refin do contrato vai para Aguardando Retorno CIP."

        df.loc[
            mask_refin_saldo,
            "observacao_regra",
        ] = "Refin da Port forcado para Aguardando Saldo porque Port + Refin do contrato vai para Aguardando Retorno CIP."

    port_refin_cip_retornada = df[
        (df["tipo_nome"] == "Port + Refin")
        & (df["numero_contrato_match"].notna())
        & (df["nw_fase_norm"] == normalize_key(FASE_CIP_RETORNADA))
    ]
    contratos_forcar_saldo = set(port_refin_cip_retornada["numero_contrato_match"].dropna())

    if contratos_forcar_saldo:
        mask_refin_saldo = (
            (df["tipo_nome"] == "Refin da Port")
            & (df["numero_contrato_match"].isin(contratos_forcar_saldo))
        )
        df.loc[mask_refin_saldo, "nw_fase"] = FASE_AGUARDANDO_SALDO
        df.loc[mask_refin_saldo, "nw_fase_id"] = FASE_AGUARDANDO_SALDO_ID
        df.loc[mask_refin_saldo, "nw_fase_norm"] = normalize_key(FASE_AGUARDANDO_SALDO)
        df.loc[mask_refin_saldo, "depara_motivo"] = MOTIVO_AGUARDANDO_SALDO_REFIN_PORT
        df.loc[mask_refin_saldo, "regra_conciliacao"] = "refin_saldo_port_cip_retornada"
        df.loc[
            mask_refin_saldo & (df["depara_status"] == "sem_depara"),
            "depara_status",
        ] = "refin_saldo_port_cip_retornada"
        df.loc[
            mask_refin_saldo,
            "observacao_regra",
        ] = "Refin da Port forcado para Aguardando Saldo porque Port + Refin do contrato vai para CIP Retornada."

    mask_port_refin = (df["tipo_nome"] == "Port + Refin") & (df["numero_contrato_match"].notna())
    mask_refin_da_port = (df["tipo_nome"] == "Refin da Port") & (df["numero_contrato_match"].notna())
    mask_pernas_port_refin = (
        df["tipo_nome"].isin(["Port + Refin", "Refin da Port"])
        & df["numero_contrato_match"].notna()
    )

    mask_refin_aguardando_saldo = (
        mask_refin_da_port
        & (df["nw_fase_norm"] == normalize_key(FASE_AGUARDANDO_SALDO))
        & (df["depara_motivo"].map(clean_text_value).isna())
    )
    df.loc[mask_refin_aguardando_saldo, "depara_motivo"] = MOTIVO_AGUARDANDO_SALDO_REFIN_PORT

    contratos_port_indo_averbacao = set(
        df.loc[
            mask_port_refin
            & (
                df["nw_fase_norm"].isin([
                    normalize_key(FASE_AGUARDANDO_AVERBACAO),
                    normalize_key(FASE_AGUARDA_AVERBACAO),
                ])
            ),
            "numero_contrato_match",
        ].dropna()
    )
    if contratos_port_indo_averbacao:
        mask_refin_saldo_por_port_averbacao = (
            mask_refin_da_port
            & df["numero_contrato_match"].isin(contratos_port_indo_averbacao)
        )
        df.loc[mask_refin_saldo_por_port_averbacao, "nw_fase"] = FASE_AGUARDANDO_SALDO
        df.loc[mask_refin_saldo_por_port_averbacao, "nw_fase_id"] = FASE_AGUARDANDO_SALDO_ID
        df.loc[mask_refin_saldo_por_port_averbacao, "nw_fase_norm"] = normalize_key(FASE_AGUARDANDO_SALDO)
        df.loc[mask_refin_saldo_por_port_averbacao, "depara_motivo"] = MOTIVO_PORT_PAGA_AGUARDANDO_AVERBACAO
        df.loc[mask_refin_saldo_por_port_averbacao, "regra_conciliacao"] = "refin_saldo_port_aguardando_averbacao"
        df.loc[
            mask_refin_saldo_por_port_averbacao & (df["depara_status"] == "sem_depara"),
            "depara_status",
        ] = "refin_saldo_port_aguardando_averbacao"
        df.loc[
            mask_refin_saldo_por_port_averbacao,
            "observacao_regra",
        ] = "Refin da Port forcado para Aguardando Saldo porque Port + Refin do contrato esta indo para Aguardando Averbacao."

    contratos_port_reprova_pos_cip = set(
        df.loc[
            mask_port_refin
            & (
                (df["nw_fase_norm"] == normalize_key(FASE_REPROVA_POS_CIP))
                | (df["crm_status_norm"] == normalize_key(FASE_REPROVA_POS_CIP))
            ),
            "numero_contrato_match",
        ].dropna()
    )
    motivos_reprova_pos_cip_por_contrato = {}
    mask_port_reprova_pos_cip_depara = (
        mask_port_refin
        & (df["nw_fase_norm_depara_original"] == normalize_key(FASE_REPROVA_POS_CIP))
    )
    for _, row_reprova in df.loc[mask_port_reprova_pos_cip_depara].iterrows():
        numero_contrato = row_reprova.get("numero_contrato_match")
        if not numero_contrato or numero_contrato in motivos_reprova_pos_cip_por_contrato:
            continue
        motivo_depara = (
            clean_text_value(row_reprova.get("depara_motivo_original"))
            if row_reprova.get("depara_status_original") == "ok_motivo"
            else None
        )
        motivos_reprova_pos_cip_por_contrato[numero_contrato] = (
            motivo_depara or MOTIVO_REPROVA_POS_CIP
        )

    if contratos_port_reprova_pos_cip:
        mask_forcar_reprova_pos_cip = (
            mask_pernas_port_refin
            & df["numero_contrato_match"].isin(contratos_port_reprova_pos_cip)
        )
        aplicar_fase_forcada(
            df,
            mask_forcar_reprova_pos_cip,
            FASE_REPROVA_POS_CIP,
            FASE_REPROVA_POS_CIP_ID,
            "reprova_pos_cip_forcada",
            "Port + Refin em Reprova Pos-CIP; Port + Refin e Refin da Port forcados para Reprova Pos-CIP e travados nessa fase.",
        )

    mask_reprova_pos_cip_travada = df["crm_status_norm"] == normalize_key(FASE_REPROVA_POS_CIP)
    aplicar_fase_forcada(
        df,
        mask_reprova_pos_cip_travada,
        FASE_REPROVA_POS_CIP,
        FASE_REPROVA_POS_CIP_ID,
        "reprova_pos_cip_travada",
        "Proposta ja esta em Reprova Pos-CIP no CRM; fase travada para nao sair desse status.",
    )

    mask_destino_reprova_pos_cip = (
        df["nw_fase_norm"] == normalize_key(FASE_REPROVA_POS_CIP)
    )
    for idx in df.index[mask_destino_reprova_pos_cip]:
        numero_contrato = df.at[idx, "numero_contrato_match"]
        motivo_reprova = motivos_reprova_pos_cip_por_contrato.get(numero_contrato)
        if not motivo_reprova:
            depara_apontava_reprova = (
                df.at[idx, "nw_fase_norm_depara_original"]
                == normalize_key(FASE_REPROVA_POS_CIP)
            )
            motivo_depara_valido = (
                clean_text_value(df.at[idx, "depara_motivo_original"])
                if (
                    depara_apontava_reprova
                    and df.at[idx, "depara_status_original"] == "ok_motivo"
                )
                else None
            )
            motivo_reprova = motivo_depara_valido or MOTIVO_REPROVA_POS_CIP
        df.at[idx, "depara_motivo"] = motivo_reprova

    mask_reprovada_cancelada_travada = df["crm_status_norm"] == normalize_key(FASE_REPROVADA_CANCELADA)
    aplicar_fase_forcada(
        df,
        mask_reprovada_cancelada_travada,
        FASE_REPROVADA_CANCELADA,
        FASE_REPROVADA_CANCELADA_ID,
        "reprovada_cancelada_travada",
        "Proposta ja esta em Reprovada - Cancelada no CRM; fase travada para nao sair desse status.",
    )

    df["crm_status_id_num"] = pd.to_numeric(get_series(df, "crm_status_id"), errors="coerce").astype("Int64")
    mask_digitado_stand_by_travada = (
        (df["crm_status_norm"] == normalize_key(FASE_DIGITADO_STAND_BY))
    )
    aplicar_fase_forcada(
        df,
        mask_digitado_stand_by_travada,
        FASE_DIGITADO_STAND_BY,
        digitado_stand_by_id,
        "digitado_stand_by_travada",
        "Proposta ja esta em Digitado Stand By no CRM; fase travada para nao sair desse status.",
    )

    contratos_aguardando_supervisor = set(
        df.loc[
            mask_pernas_port_refin
            & (df["fase_id"] == FASE_AGUARDANDO_APROVACAO_SUPERVISOR_ID),
            "numero_contrato_match",
        ].dropna()
    )
    contratos_supervisor_liberados_por_assinatura = set(
        df.loc[
            mask_pernas_port_refin
            & (df["fase_id"] == FASE_AGUARDANDO_APROVACAO_SUPERVISOR_ID)
            & (df["crm_status_norm"] == normalize_key(FASE_ASSINATURA_REALIZADA)),
            "numero_contrato_match",
        ].dropna()
    )
    mask_supervisor_liberado_por_assinatura = (
        mask_pernas_port_refin
        & df["numero_contrato_match"].isin(contratos_supervisor_liberados_por_assinatura)
    )
    aplicar_fase_forcada(
        df,
        mask_supervisor_liberado_por_assinatura,
        FASE_EM_ANALISE_ACOMPANHAMENTO,
        FASE_EM_ANALISE_ACOMPANHAMENTO_ID,
        "aguardando_supervisor_liberado_por_assinatura",
        "Banco retornou Aguardando aprovacao do Supervisor e o CRM estava em Assinatura Realizada; as duas pernas seguem para Em analise - Acompanhamento.",
        MOTIVO_AGUARDANDO_APROVACAO,
    )
    mask_banco_aguardando_supervisor = (
        (
            (df["fase_id"] == FASE_AGUARDANDO_APROVACAO_SUPERVISOR_ID)
            | (
                mask_pernas_port_refin
                & df["numero_contrato_match"].isin(contratos_aguardando_supervisor)
            )
        )
        & ~mask_supervisor_liberado_por_assinatura
    )

    regras_fases_travadas = [
        "reprova_pos_cip_forcada",
        "reprova_pos_cip_travada",
        "reprovada_cancelada_travada",
        "digitado_stand_by_travada",
        "integrado_travado",
        "aguardando_supervisor_liberado_por_assinatura",
    ]
    fases_adicionais_travadas = [
        (
            FASE_FRAUDE_DECTADA,
            lookup_nw_fase_id(df_fases, FASE_FRAUDE_DECTADA),
            "fraude_dectada_travada",
            "Proposta ja esta em Fraude Dectada no CRM; fase travada para nao sair desse status.",
        ),
        (
            FASE_INCONSISTENCIA_POS_PAGAMENTO,
            lookup_nw_fase_id(df_fases, FASE_INCONSISTENCIA_POS_PAGAMENTO),
            "inconsistencia_pos_pagamento_travada",
            "Proposta ja esta em Inconsistencia Pos Pagamento no CRM; fase travada para nao sair desse status.",
        ),
        (
            FASE_TRATATIVA_FINALIZADA,
            lookup_nw_fase_id(df_fases, FASE_TRATATIVA_FINALIZADA),
            "tratativa_finalizada_travada",
            "Proposta ja esta em Tratativa Finalizada no CRM; fase travada para nao sair desse status.",
        ),
    ]
    for fase_travada, fase_travada_id, regra_travada, observacao_travada in fases_adicionais_travadas:
        aplicar_fase_forcada(
            df,
            df["crm_status_norm"] == normalize_key(fase_travada),
            fase_travada,
            fase_travada_id,
            regra_travada,
            observacao_travada,
        )
        regras_fases_travadas.append(regra_travada)

    mask_reapresentacao_pagamento = df["crm_status_norm"] == normalize_key(FASE_REAPRESENTACAO_PAGAMENTO)
    mask_reapresentacao_pagamento_para_integrado = (
        mask_reapresentacao_pagamento
        & (df["nw_fase_norm"] == normalize_key(FASE_INTEGRADO))
    )
    df.loc[
        mask_reapresentacao_pagamento_para_integrado,
        "regra_conciliacao",
    ] = "reapresentacao_pagamento_para_integrado_liberada"
    df.loc[
        mask_reapresentacao_pagamento_para_integrado,
        "observacao_regra",
    ] = "Proposta esta em Reapresentacao de Pagamento no CRM; liberada apenas porque o destino calculado e Integrado."

    mask_reapresentacao_pagamento_travada = (
        mask_reapresentacao_pagamento
        & (df["nw_fase_norm"] != normalize_key(FASE_INTEGRADO))
    )
    aplicar_fase_forcada(
        df,
        mask_reapresentacao_pagamento_travada,
        FASE_REAPRESENTACAO_PAGAMENTO,
        lookup_nw_fase_id(df_fases, FASE_REAPRESENTACAO_PAGAMENTO),
        "reapresentacao_pagamento_travada",
        "Proposta ja esta em Reapresentacao de Pagamento no CRM; fase travada, exceto quando o destino for Integrado.",
    )
    regras_fases_travadas.extend([
        "reapresentacao_pagamento_para_integrado_liberada",
        "reapresentacao_pagamento_travada",
    ])

    mask_tratativa_comercial = df["crm_status_norm"] == normalize_key(FASE_TRATATIVA_COMERCIAL)
    mask_tratativa_comercial_para_integrado = (
        mask_tratativa_comercial
        & (df["nw_fase_norm"] == normalize_key(FASE_INTEGRADO))
    )
    df.loc[mask_tratativa_comercial_para_integrado, "regra_conciliacao"] = "tratativa_comercial_para_integrado_liberada"
    df.loc[
        mask_tratativa_comercial_para_integrado,
        "observacao_regra",
    ] = "Proposta esta em Tratativa Comercial no CRM; liberada apenas porque o destino calculado e Integrado."

    tratativa_comercial_id = lookup_nw_fase_id(df_fases, FASE_TRATATIVA_COMERCIAL)
    mask_tratativa_comercial_travada = (
        mask_tratativa_comercial
        & (df["nw_fase_norm"] != normalize_key(FASE_INTEGRADO))
    )
    aplicar_fase_forcada(
        df,
        mask_tratativa_comercial_travada,
        FASE_TRATATIVA_COMERCIAL,
        tratativa_comercial_id,
        "tratativa_comercial_travada",
        "Proposta ja esta em Tratativa Comercial no CRM; fase travada, exceto quando o destino for Integrado.",
    )
    regras_fases_travadas.extend([
        "tratativa_comercial_para_integrado_liberada",
        "tratativa_comercial_travada",
    ])

    contratos_refin_integrado = set(
        df.loc[
            mask_refin_da_port
            & (df["nw_fase_norm"] == normalize_key(FASE_INTEGRADO)),
            "numero_contrato_match",
        ].dropna()
    )

    contratos_port_integrado_banco = set(
        df.loc[
            mask_port_refin
            & (df["nw_fase_norm"] == normalize_key(FASE_INTEGRADO)),
            "numero_contrato_match",
        ].dropna()
    )
    contratos_ambas_pernas_integrado = contratos_port_integrado_banco & contratos_refin_integrado
    contratos_refin_averbacao_com_port_integrada = contratos_port_integrado_banco - contratos_refin_integrado
    if contratos_refin_averbacao_com_port_integrada:
        mask_forcar_refin_averbacao = (
            mask_refin_da_port
            & df["numero_contrato_match"].isin(contratos_refin_averbacao_com_port_integrada)
            & (df["nw_fase_norm"] != normalize_key(FASE_INTEGRADO))
            & (df["depara_status_origem"].apply(normalize_text) != normalize_text(STATUS_AVERBADO_PENDENTE_ANUENCIA))
            & (df["crm_status_norm"] != normalize_key(FASE_INTEGRADO))
            & (~df["regra_conciliacao"].isin(regras_fases_travadas))
        )
        aplicar_fase_forcada(
            df,
            mask_forcar_refin_averbacao,
            FASE_AGUARDANDO_AVERBACAO,
            aguardando_averbacao_id,
            "refin_averbacao_com_port_integrada_banco",
            "Port + Refin veio do banco/depara como Integrado; Refin da Port do mesmo contrato foi direcionada para Aguardando Averbacao.",
            MOTIVO_PORTABILIDADE_AVERBADA,
        )
        mask_manter_port_averbacao = (
            mask_port_refin
            & df["numero_contrato_match"].isin(contratos_refin_averbacao_com_port_integrada)
            & (df["crm_status_norm"] != normalize_key(FASE_INTEGRADO))
            & (~df["regra_conciliacao"].isin(regras_fases_travadas))
        )
        aplicar_fase_forcada(
            df,
            mask_manter_port_averbacao,
            FASE_AGUARDANDO_AVERBACAO,
            aguardando_averbacao_id,
            "port_averbacao_com_refin_nao_integrada",
            "Port + Refin veio do banco/depara como Integrado, mas Refin da Port ainda nao integrou; Port + Refin permanece em Aguardando Averbacao.",
            MOTIVO_PORTABILIDADE_AVERBADA,
        )

    mask_integrado_travado = df["crm_status_norm"] == normalize_key(FASE_INTEGRADO)
    aplicar_fase_forcada(
        df,
        mask_integrado_travado,
        FASE_INTEGRADO,
        FASE_INTEGRADO_ID,
        "integrado_travado",
        "Proposta ja esta em Integrado no CRM; fase travada para nao sair desse status.",
    )

    df["resultado"] = "falta_atualizar"
    df.loc[df["proposta_id"].isna(), "resultado"] = "sem_proposta_incremental"
    df.loc[df["depara_status"] == "sem_depara", "resultado"] = "sem_depara_fase"
    df.loc[
        (df["proposta_id"].notna())
        & (df["depara_status"] != "sem_depara")
        & (df["nw_fase_norm"] == df["crm_status_norm"]),
        "resultado",
    ] = "igual"

    df.loc[
        (df["proposta_id"].notna())
        & (df["depara_status"] != "sem_depara")
        & (df["nw_fase_norm"] != df["crm_status_norm"]),
        "resultado",
    ] = "falta_atualizar"

    motivos_substatus_protegidos_norm = {
        normalize_text(motivo)
        for motivo in MOTIVOS_SUBSTATUS_PROTEGIDOS
    }
    fases_substatus_protegido_norm = {
        normalize_key(FASE_AGUARDANDO_SALDO),
        normalize_key(FASE_AGUARDANDO_RETORNO_CIP),
    }
    mask_motivo_substatus_protegido = (
        (df["crm_status_norm"].isin(fases_substatus_protegido_norm))
        & (df["crm_substatus_norm"].isin(motivos_substatus_protegidos_norm))
    )
    df["motivo_substatus_protegido"] = mask_motivo_substatus_protegido
    df.loc[mask_motivo_substatus_protegido, "depara_motivo"] = df.loc[
        mask_motivo_substatus_protegido,
        "crm_substatus",
    ]
    df.loc[
        mask_motivo_substatus_protegido,
        "observacao_regra",
    ] = "Motivo atual protegido em Aguardando Saldo/Aguardando Retorno CIP; substatus nao sera alterado pela conciliacao."

    regras_refin_saldo_com_motivo = {
        "refin_saldo_port_aguardando_cip",
        "refin_saldo_port_cip_retornada",
        "refin_saldo_port_aguardando_averbacao",
    }
    regras_com_motivo_forcado = regras_refin_saldo_com_motivo | {
        "port_averbacao_com_refin_nao_integrada",
        "refin_averbacao_portabilidade_averbada",
    }
    mask_substatus_forcado_diferente = (
        (df["proposta_id"].notna())
        & (df["regra_conciliacao"].isin(regras_com_motivo_forcado))
        & (df["nw_fase_norm"] == df["crm_status_norm"])
        & (df["crm_substatus_norm"] != get_series(df, "depara_motivo").apply(normalize_text))
    )
    df.loc[mask_substatus_forcado_diferente, "resultado"] = "falta_atualizar"
    df.loc[
        mask_substatus_forcado_diferente,
        "observacao_regra",
    ] = "Fase ja esta igual no CRM, mas substatus esta diferente do motivo calculado; fase e motivo serao reenviados."

    mask_integracao_isolada = (
        (df["resultado"] == "falta_atualizar")
        & mask_pernas_port_refin
        & (df["nw_fase_norm"] == normalize_key(FASE_INTEGRADO))
        & (~df["numero_contrato_match"].isin(contratos_ambas_pernas_integrado))
    )
    df.loc[mask_integracao_isolada, "resultado"] = "alteracao_barrada"
    df.loc[mask_integracao_isolada, "regra_conciliacao"] = "integracao_isolada_barrada"
    df.loc[
        mask_integracao_isolada,
        "observacao_regra",
    ] = "Integracao barrada: Port + Refin e Refin da Port so podem ir para Integrado quando ambas as pernas do contrato vierem como Integrado no banco/depara."

    mask_inconsistencia_cip_para_cip_retornada = (
        (df["resultado"] == "falta_atualizar")
        & (df["crm_status_norm"] == normalize_key(FASE_INCONSISTENCIA_CIP_RETORNADA))
        & (df["nw_fase_norm"] == normalize_key(FASE_CIP_RETORNADA))
    )
    df.loc[mask_inconsistencia_cip_para_cip_retornada, "resultado"] = "alteracao_barrada"
    df.loc[
        mask_inconsistencia_cip_para_cip_retornada,
        "regra_conciliacao",
    ] = "inconsistencia_cip_para_cip_retornada_barrada"
    df.loc[
        mask_inconsistencia_cip_para_cip_retornada,
        "observacao_regra",
    ] = "Proposta em Inconsistencia CIP retornada no CRM nao pode ser atualizada para CIP Retornada pela conciliacao."

    df.loc[mask_banco_aguardando_supervisor, "resultado"] = "alteracao_barrada"
    df.loc[mask_banco_aguardando_supervisor, "regra_conciliacao"] = "aguardando_supervisor_banco_barrada"
    df.loc[
        mask_banco_aguardando_supervisor,
        "observacao_regra",
    ] = "Banco retornou Aguardando aprovacao do Supervisor em uma proposta ou perna do contrato; conciliacao bloqueada para nao atualizar fase em qualquer produto ou nas duas pernas Port + Refin/Refin da Port."

    mask_port_averbacao_substatus_diferente = (
        (df["proposta_id"].notna())
        & (df["regra_conciliacao"] == "port_aguardando_averbacao_forcada")
        & (df["nw_fase_norm"] == df["crm_status_norm"])
        & (df["crm_substatus_norm"] != normalize_text(MOTIVO_AGUARDANDO_AVERBACAO_PORT))
    )
    df.loc[mask_port_averbacao_substatus_diferente, "resultado"] = "falta_atualizar"
    df.loc[
        mask_port_averbacao_substatus_diferente,
        "observacao_regra",
    ] = "Port + Refin ja esta em Aguardando Averbacao, mas substatus do CRM esta diferente do motivo travado; fase e motivo serao reenviados."

    mask_port_averbacao_dentro_prazo = (
        (df["resultado"] == "falta_atualizar")
        & (df["produto_norm"] == "PORTABILIDADE")
        & (df["tipo_nome"] != "Refin da Port")
        & (df["regra_conciliacao"] != "port_averbacao_com_refin_nao_integrada")
        & (
            df["nw_fase_norm"].isin([
                normalize_key(FASE_AGUARDANDO_AVERBACAO),
                normalize_key(FASE_AGUARDA_AVERBACAO),
            ])
        )
    )
    df.loc[mask_port_averbacao_dentro_prazo, "depara_motivo"] = MOTIVO_AGUARDANDO_AVERBACAO_PORT
    df.loc[mask_port_averbacao_dentro_prazo, "regra_conciliacao"] = "port_averbacao_dentro_prazo"
    df.loc[
        mask_port_averbacao_dentro_prazo,
        "observacao_regra",
    ] = "Portabilidade com destino Aguardando Averbacao recebe motivo Ag. Averbacao - Port (Dentro do prazo)."

    mask_refin_averbacao_motivo_port = (
        mask_refin_da_port
        & (
            df["nw_fase_norm"].isin([
                normalize_key(FASE_AGUARDANDO_AVERBACAO),
                normalize_key(FASE_AGUARDA_AVERBACAO),
            ])
        )
        & (get_series(df, "depara_motivo").apply(normalize_text) == normalize_text(MOTIVO_AGUARDANDO_AVERBACAO_PORT))
    )
    df.loc[mask_refin_averbacao_motivo_port, "depara_motivo"] = None
    df.loc[
        mask_refin_averbacao_motivo_port & (df["regra_conciliacao"] == "port_averbacao_dentro_prazo"),
        "regra_conciliacao",
    ] = "normal"
    df.loc[
        mask_refin_averbacao_motivo_port,
        "observacao_regra",
    ] = "Refin da Port com destino Aguardando Averbacao nunca recebe motivo Ag. Averbacao - Port (Dentro do prazo)."

    mask_refin_averbacao_sem_motivo = (
        mask_refin_da_port
        & (
            df["nw_fase_norm"].isin([
                normalize_key(FASE_AGUARDANDO_AVERBACAO),
                normalize_key(FASE_AGUARDA_AVERBACAO),
            ])
        )
        & (get_series(df, "depara_motivo").map(clean_text_value).isna())
    )
    df.loc[mask_refin_averbacao_sem_motivo, "depara_motivo"] = MOTIVO_PORTABILIDADE_AVERBADA
    df.loc[mask_refin_averbacao_sem_motivo, "regra_conciliacao"] = "refin_averbacao_portabilidade_averbada"
    df.loc[
        mask_refin_averbacao_sem_motivo,
        "observacao_regra",
    ] = "Refin da Port com destino Aguardando Averbacao e sem motivo definido recebe motivo Portabilidade averbada."

    mask_refin_saldo_para_averbacao_sem_port_integrado = (
        (df["resultado"] == "falta_atualizar")
        & mask_refin_da_port
        & (df["crm_status_norm"] == normalize_key(FASE_AGUARDANDO_SALDO))
        & (
            df["nw_fase_norm"].isin([
                normalize_key(FASE_AGUARDANDO_AVERBACAO),
                normalize_key(FASE_AGUARDA_AVERBACAO),
            ])
        )
        & (~df["numero_contrato_match"].isin(contratos_port_integrado_banco))
    )
    df.loc[mask_refin_saldo_para_averbacao_sem_port_integrado, "resultado"] = "alteracao_barrada"
    df.loc[mask_refin_saldo_para_averbacao_sem_port_integrado, "regra_conciliacao"] = "alteracao_barrada"
    df.loc[
        mask_refin_saldo_para_averbacao_sem_port_integrado,
        "observacao_regra",
    ] = "Refin da Port em Aguardando Saldo so pode ir para Aguardando Averbacao quando a perna Port + Refin do contrato veio do banco/depara como Integrado."

    mask_barra_port_averbacao = (
        (df["resultado"] == "falta_atualizar")
        & (df["produto_norm"] == "PORTABILIDADE")
        & (df["crm_status_norm"] == normalize_key(FASE_AGUARDANDO_AVERBACAO))
        & (df["nw_fase_norm"] != normalize_key(FASE_INTEGRADO))
        & (df["regra_conciliacao"] != "reprova_pos_cip_forcada")
        & ~(
            mask_refin_da_port
            & (df["nw_fase_norm"] == normalize_key(FASE_AGUARDANDO_SALDO))
            & (df["regra_conciliacao"].isin([
                "refin_saldo_port_aguardando_cip",
                "refin_saldo_port_cip_retornada",
                "refin_saldo_port_aguardando_averbacao",
            ]))
        )
    )
    df.loc[mask_barra_port_averbacao, "resultado"] = "alteracao_barrada"
    df.loc[mask_barra_port_averbacao, "regra_conciliacao"] = "alteracao_barrada"
    df.loc[
        mask_barra_port_averbacao,
        "observacao_regra",
    ] = "Portabilidade em Aguardando Averbação no CRM só pode atualizar se a fase destino for Integrado."

    fases_reprovacao_bloqueio_norm = {normalize_key(fase) for fase in FASES_REPROVACAO_BLOQUEIO}
    contratos_bloquear_reprovacao = set(
        df.loc[
            (df["resultado"] == "falta_atualizar")
            & (df["tipo_nome"].isin(["Port + Refin", "Refin da Port"]))
            & (df["numero_contrato_match"].notna())
            & (
                (df["nw_fase_norm"].isin(fases_reprovacao_bloqueio_norm))
                | (df["depara_status_origem_norm"].isin(fases_reprovacao_bloqueio_norm))
                | (df["crm_status_norm"] == normalize_key(FASE_REPROVA_POS_CIP))
            ),
            "numero_contrato_match",
        ].dropna()
    )
    if contratos_bloquear_reprovacao:
        mask_bloqueio_reprovacao_pernas = (
            (df["resultado"] == "falta_atualizar")
            & (df["tipo_nome"].isin(["Port + Refin", "Refin da Port"]))
            & (df["numero_contrato_match"].isin(contratos_bloquear_reprovacao))
        )
        df.loc[mask_bloqueio_reprovacao_pernas, "resultado"] = "alteracao_barrada"
        df.loc[mask_bloqueio_reprovacao_pernas, "regra_conciliacao"] = "alteracao_barrada"
        df.loc[
            mask_bloqueio_reprovacao_pernas,
            "observacao_regra",
        ] = "Contrato com Port + Refin/Refin da Port em fluxo de reprovacao/cancelamento; bloqueadas as duas pernas."

    mask_substatus_portabilidade_finalizada = (
        (df["resultado"] == "falta_atualizar")
        & (df["nw_fase_norm"] == normalize_key(FASE_REPROVADA_CANCELADA))
        & (
            get_series(df, "depara_motivo").apply(normalize_text)
            == normalize_text(MOTIVO_PORTABILIDADE_FINALIZADA_CONTRATO)
        )
    )
    df.loc[
        mask_substatus_portabilidade_finalizada,
        "depara_motivo",
    ] = MOTIVO_PORTABILIDADE_FINALIZADA

    mask_margem_negativa_bloqueada = (
        (df["resultado"] == "falta_atualizar")
        & (df["crm_status_norm"] == normalize_key(FASE_MARGEM_NEGATIVA))
    )
    df.loc[mask_margem_negativa_bloqueada, "resultado"] = "alteracao_barrada"
    df.loc[
        mask_margem_negativa_bloqueada,
        "regra_conciliacao",
    ] = "margem_negativa_bloqueada"
    df.loc[
        mask_margem_negativa_bloqueada,
        "observacao_regra",
    ] = "Proposta em Margem Negativa – Saldo Pago no CRM; conciliacao bloqueada para nao alterar a fase."

    df["api_method"] = None
    df["api_endpoint"] = None
    df["update_payload_erro"] = df.apply(build_update_payload_error, axis=1)
    df["update_payload"] = df.apply(build_update_payload, axis=1)
    mask_tem_payload = df["update_payload"].notna()
    df.loc[mask_tem_payload, "api_method"] = "PUT"
    df.loc[mask_tem_payload, "api_endpoint"] = df.loc[mask_tem_payload, "proposta_id"].map(
        lambda proposta_id: f"https://developers.newcorban.com.br/v1/proposals/{int(proposta_id)}/status"
        if pd.notna(proposta_id)
        else None
    )
    df["update_payload_json"] = df["update_payload"].map(
        lambda payload: json.dumps(payload, ensure_ascii=False, default=str) if isinstance(payload, dict) else None
    )
    df["update_dates_json"] = df["update_payload"].map(
        lambda payload: json.dumps(payload.get("dates", {}), ensure_ascii=False, default=str)
        if isinstance(payload, dict)
        else None
    )
    df["formalizer_payload"] = df.apply(build_clear_formalizer_payload, axis=1)
    df["formalizer_endpoint"] = None
    mask_tem_formalizer_payload = df["formalizer_payload"].notna()
    df.loc[mask_tem_formalizer_payload, "formalizer_endpoint"] = df.loc[
        mask_tem_formalizer_payload, "proposta_id"
    ].map(
        lambda proposta_id: f"https://developers.newcorban.com.br/v1/proposals/{int(proposta_id)}"
        if pd.notna(proposta_id)
        else None
    )
    df["formalizer_payload_json"] = df["formalizer_payload"].map(
        lambda payload: json.dumps(payload, ensure_ascii=False, default=str) if isinstance(payload, dict) else None
    )
    df["substatus_payload"] = df.apply(build_substatus_payload, axis=1)
    df["substatus_endpoint"] = None
    mask_tem_substatus_payload = df["substatus_payload"].notna()
    df.loc[mask_tem_substatus_payload, "substatus_endpoint"] = df.loc[
        mask_tem_substatus_payload, "proposta_id"
    ].map(
        lambda proposta_id: f"https://developers.newcorban.com.br/v1/proposals/{int(proposta_id)}"
        if pd.notna(proposta_id)
        else None
    )
    df["substatus_payload_json"] = df["substatus_payload"].map(
        lambda payload: json.dumps(payload, ensure_ascii=False, default=str) if isinstance(payload, dict) else None
    )
    df["api_executado"] = False
    df["api_status_code"] = None
    df["api_sucesso"] = None
    df["api_response_json"] = None
    df["api_response_text"] = None
    df["api_erro"] = None
    df["api_ratelimit_limit"] = None
    df["api_ratelimit_remaining"] = None
    df["api_ratelimit_reset"] = None
    df["api_sleep_seconds"] = None
    df["formalizer_api_executado"] = False
    df["formalizer_status_code"] = None
    df["formalizer_sucesso"] = None
    df["formalizer_response_json"] = None
    df["formalizer_response_text"] = None
    df["formalizer_erro"] = None
    df["formalizer_ratelimit_remaining"] = None
    df["formalizer_sleep_seconds"] = None
    df["substatus_api_executado"] = False
    df["substatus_status_code"] = None
    df["substatus_sucesso"] = None
    df["substatus_response_json"] = None
    df["substatus_response_text"] = None
    df["substatus_erro"] = None
    df["substatus_ratelimit_remaining"] = None
    df["substatus_sleep_seconds"] = None

    if executar_api:
        if not token:
            raise ValueError("NEWCORBAN_API nao definido. Necessario para executar atualizacao via API.")

        payload_indexes = list(df.index[df["update_payload"].notna()])
        logger.info("[API STATUS] Total de payloads para envio: %s", len(payload_indexes))

        for pos, idx in enumerate(payload_indexes, start=1):
            if pos > 1:
                logger.info("[API STATUS] Aguardando %.1fs antes da requisicao %s/%s.", API_MIN_DELAY_SECONDS, pos, len(payload_indexes))
                time.sleep(API_MIN_DELAY_SECONDS)

            if isinstance(df.at[idx, "formalizer_payload"], dict):
                logger.info(
                    "[API FORMALIZADOR] Limpando formalizador antes da troca de status proposta_id=%s.",
                    df.at[idx, "proposta_id"],
                )
                formalizer_result = call_clear_formalizer_api(df.loc[idx], token)
                for col, value in formalizer_result.items():
                    df.at[idx, col] = value
                logger.info(
                    "[API FORMALIZADOR] proposta_id=%s status_code=%s sucesso=%s remaining=%s erro=%s",
                    df.at[idx, "proposta_id"],
                    formalizer_result.get("formalizer_status_code"),
                    formalizer_result.get("formalizer_sucesso"),
                    formalizer_result.get("formalizer_ratelimit_remaining"),
                    formalizer_result.get("formalizer_erro"),
                )

                formalizer_sleep = formalizer_result.get("formalizer_sleep_seconds")
                if pos < len(payload_indexes) and formalizer_sleep and formalizer_sleep > API_MIN_DELAY_SECONDS:
                    logger.info("[API FORMALIZADOR] Rate limit baixo; aguardando %.1fs.", formalizer_sleep)
                    time.sleep(float(formalizer_sleep))

                if not formalizer_result.get("formalizer_sucesso"):
                    df.at[idx, "api_executado"] = False
                    df.at[idx, "api_sucesso"] = False
                    df.at[idx, "api_erro"] = "Status nao enviado porque a limpeza do formalizador falhou."
                    logger.info(
                        "[API STATUS] proposta_id=%s nao enviada; limpeza do formalizador falhou.",
                        df.at[idx, "proposta_id"],
                    )
                    continue

                logger.info(
                    "[API STATUS] Aguardando %.1fs antes de trocar status proposta_id=%s.",
                    API_MIN_DELAY_SECONDS,
                    df.at[idx, "proposta_id"],
                )
                time.sleep(API_MIN_DELAY_SECONDS)

            payload_status = df.at[idx, "update_payload"]
            logger.info(
                "[API STATUS] ENVIANDO proposta_id=%s fase=%s fase_id=%s substatus=%s",
                df.at[idx, "proposta_id"],
                df.at[idx, "nw_fase"],
                payload_status.get("status_id") if isinstance(payload_status, dict) else None,
                payload_status.get("substatus") if isinstance(payload_status, dict) else None,
            )

            api_result = call_status_api(df.loc[idx], token)
            for col, value in api_result.items():
                df.at[idx, col] = value
            logger.info(
                "[API STATUS] %s/%s proposta_id=%s fase=%s fase_id=%s substatus=%s status_code=%s sucesso=%s remaining=%s reset=%s erro=%s",
                pos,
                len(payload_indexes),
                df.at[idx, "proposta_id"],
                df.at[idx, "nw_fase"],
                payload_status.get("status_id") if isinstance(payload_status, dict) else None,
                payload_status.get("substatus") if isinstance(payload_status, dict) else None,
                api_result.get("api_status_code"),
                api_result.get("api_sucesso"),
                api_result.get("api_ratelimit_remaining"),
                api_result.get("api_ratelimit_reset"),
                api_result.get("api_erro"),
            )
            sleep_seconds = api_result.get("api_sleep_seconds")
            if pos < len(payload_indexes) and sleep_seconds and sleep_seconds > API_MIN_DELAY_SECONDS:
                logger.info("[API STATUS] Rate limit baixo; aguardando %.1fs.", sleep_seconds)
                time.sleep(float(sleep_seconds))

            if (
                api_result.get("api_sucesso")
                and isinstance(df.at[idx, "substatus_payload"], dict)
                and not (
                    isinstance(payload_status, dict)
                    and payload_status.get("substatus")
                )
            ):
                logger.info(
                    "[API SUBSTATUS] Aguardando %.1fs antes de atualizar substatus proposta_id=%s.",
                    API_MIN_DELAY_SECONDS,
                    df.at[idx, "proposta_id"],
                )
                time.sleep(API_MIN_DELAY_SECONDS)

                substatus_result = call_substatus_api(df.loc[idx], token)
                for col, value in substatus_result.items():
                    df.at[idx, col] = value
                logger.info(
                    "[API SUBSTATUS] proposta_id=%s status_code=%s sucesso=%s remaining=%s erro=%s",
                    df.at[idx, "proposta_id"],
                    substatus_result.get("substatus_status_code"),
                    substatus_result.get("substatus_sucesso"),
                    substatus_result.get("substatus_ratelimit_remaining"),
                    substatus_result.get("substatus_erro"),
                )

                substatus_sleep = substatus_result.get("substatus_sleep_seconds")
                if pos < len(payload_indexes) and substatus_sleep and substatus_sleep > API_MIN_DELAY_SECONDS:
                    logger.info("[API SUBSTATUS] Rate limit baixo; aguardando %.1fs.", substatus_sleep)
                    time.sleep(float(substatus_sleep))

    colunas = [
        "resultado",
        "af_match",
        "af",
        "numero_ade",
        "numero_contrato",
        "fase",
        "fase_id",
        "tipo_id",
        "tipo_nome",
        "produto_norm",
        "nota_status",
        "depara_status",
        "depara_chave",
        "depara_qtd_candidatos",
        "depara_status_origem",
        "depara_motivo",
        "depara_produto",
        "nw_fase",
        "nw_fase_id",
        "regra_conciliacao",
        "motivo_substatus_protegido",
        "observacao_regra",
        "proposta_id",
        "bank_proposal_number",
        "crm_status_id",
        "crm_status_name",
        "crm_substatus",
        "customer_name",
        "customer_cpf",
        "product_name",
        "bank_status",
        "dataStatus",
        "data_retorno_cip",
        "api_method",
        "api_endpoint",
        "update_payload_erro",
        "update_payload_json",
        "update_dates_json",
        "api_executado",
        "api_status_code",
        "api_sucesso",
        "api_response_json",
        "api_response_text",
        "api_erro",
        "api_ratelimit_limit",
        "api_ratelimit_remaining",
        "api_ratelimit_reset",
        "api_sleep_seconds",
        "formalizer_endpoint",
        "formalizer_payload_json",
        "formalizer_api_executado",
        "formalizer_status_code",
        "formalizer_sucesso",
        "formalizer_response_json",
        "formalizer_response_text",
        "formalizer_erro",
        "formalizer_ratelimit_remaining",
        "formalizer_sleep_seconds",
        "substatus_endpoint",
        "substatus_payload_json",
        "substatus_api_executado",
        "substatus_status_code",
        "substatus_sucesso",
        "substatus_response_json",
        "substatus_response_text",
        "substatus_erro",
        "substatus_ratelimit_remaining",
        "substatus_sleep_seconds",
        "proposal_updated_at",
        "synced_at",
        "consultor",
        "plataforma",
        "cpf",
        "beneficio",
    ]

    for col in colunas:
        if col not in df.columns:
            df[col] = None

    return df[colunas].sort_values(["resultado", "af_match"], na_position="last")


def export_excel(df: pd.DataFrame, output_path: str) -> str:
    path = Path(output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    df_export = df.loc[
        df["resultado"] == "falta_atualizar",
        [
            "proposta_id",
            "bank_proposal_number",
            "crm_status_name",
            "nw_fase",
            "crm_substatus",
            "depara_motivo",
        ],
    ].copy()
    df_export.columns = [
        "proposta_sistema",
        "proposta_banco_ade",
        "fase_atual",
        "fase_destino",
        "substatus_atual",
        "substatus_destino",
    ]
    df_export["proposta_sistema"] = pd.to_numeric(
        df_export["proposta_sistema"], errors="coerce"
    ).astype("Int64")

    with pd.ExcelWriter(path, engine="openpyxl") as writer:
        df_export.to_excel(writer, sheet_name="propostas_atualizar", index=False)

    logger.info("[CONCILIACAO] XLSX gerado em: %s", path)
    return str(path)


def registrar_auditoria_atualizacoes(pg: PostgresHook, df: pd.DataFrame) -> int:
    if df is None or df.empty:
        return 0

    mask_atualizado = (df["api_sucesso"] == True) | (df["substatus_sucesso"] == True)
    df_audit = df.loc[mask_atualizado].copy()
    if df_audit.empty:
        logger.info("[AUDITORIA ATUALIZACOES] Nenhuma atualizacao bem-sucedida para registrar.")
        return 0

    insert_sql = f"""
        INSERT INTO {AUDITORIA_ATUALIZACOES_TABLE} (
            proposta_id,
            af,
            cpf,
            fase_atual,
            fase_destino,
            substatus_atual,
            substatus_destino,
            tipo_produto
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    rows = []
    proposta_ids = []
    for _, row in df_audit.iterrows():
        proposta_id = clean_payload_value(row.get("proposta_id"))
        if proposta_id is not None:
            proposta_ids.append(proposta_id)

        rows.append((
            proposta_id,
            clean_text_value(row.get("af_match")) or clean_text_value(row.get("af")),
            clean_text_value(row.get("cpf")) or clean_text_value(row.get("customer_cpf")),
            clean_text_value(row.get("crm_status_name")),
            clean_text_value(row.get("nw_fase")),
            clean_text_value(row.get("crm_substatus")),
            clean_text_value(row.get("depara_motivo")),
            clean_text_value(row.get("tipo_nome")) or clean_text_value(row.get("produto_norm")),
        ))

    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"ALTER TABLE {AUDITORIA_ATUALIZACOES_TABLE} "
                "ADD COLUMN IF NOT EXISTS tipo_produto TEXT;"
            )
            if proposta_ids:
                cur.execute(
                    f"DELETE FROM {AUDITORIA_ATUALIZACOES_TABLE} WHERE proposta_id = ANY(%s);",
                    (list(set(proposta_ids)),),
                )
            cur.executemany(insert_sql, rows)
        conn.commit()
        logger.info("[AUDITORIA ATUALIZACOES] %s registros inseridos.", len(rows))
        return len(rows)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def gerar_conciliacao_att_interno(**context):
    dag_run = context.get("dag_run")
    output_dir = DEFAULT_OUTPUT_DIR
    output_file = OUTPUT_FILE_NAME
    executar_api = parse_bool(os.getenv("NEWCORBAN_EXECUTAR_API", "true"))

    if dag_run and getattr(dag_run, "conf", None):
        output_dir = dag_run.conf.get("output_dir", output_dir)
        output_file = dag_run.conf.get("output_file", output_file)
        executar_api = parse_bool(dag_run.conf.get("executar_api", executar_api))

    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    df_att, df_fases, df_prop = load_dataframes(pg)

    logger.info(
        "[CONCILIACAO] att=%s fases=%s propostas=%s",
        len(df_att),
        len(df_fases),
        len(df_prop),
    )
    logger.info("[CONCILIACAO] executar_api=%s", executar_api)

    df = build_conciliacao(
        df_att,
        df_fases,
        df_prop,
        executar_api=executar_api,
        token=NEWCORBAN_TOKEN,
    )
    auditoria_atualizacoes = registrar_auditoria_atualizacoes(pg, df) if executar_api else 0
    output_path = str(Path(output_dir) / output_file)
    export_path = export_excel(df, output_path)

    resumo = df["resultado"].value_counts(dropna=False).to_dict()
    logger.info("[CONCILIACAO] Resumo: %s", resumo)

    return {
        "arquivo": export_path,
        "total": int(len(df)),
        "iguais": int((df["resultado"] == "igual").sum()),
        "falta_atualizar": int((df["resultado"] == "falta_atualizar").sum()),
        "alteracao_barrada": int((df["resultado"] == "alteracao_barrada").sum()),
        "api_executados": int((df["api_executado"] == True).sum()),
        "api_sucessos": int((df["api_sucesso"] == True).sum()),
        "formalizer_payloads": int(df["formalizer_payload_json"].notna().sum()),
        "formalizer_executados": int((df["formalizer_api_executado"] == True).sum()),
        "formalizer_sucessos": int((df["formalizer_sucesso"] == True).sum()),
        "substatus_payloads": int(df["substatus_payload_json"].notna().sum()),
        "substatus_executados": int((df["substatus_api_executado"] == True).sum()),
        "substatus_sucessos": int((df["substatus_sucesso"] == True).sum()),
        "auditoria_atualizacoes": int(auditoria_atualizacoes),
        "sem_depara": int((df["resultado"] == "sem_depara_fase").sum()),
        "sem_proposta": int((df["resultado"] == "sem_proposta_incremental").sum()),
    }


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    description="Concilia fases do attInterno com propostas incrementais NewCorban e exporta XLSX.",
    tags=["newcorban", "att-interno", "conciliacao", "excel"],
) as dag:
    gerar_conciliacao_task = PythonOperator(
        task_id="gerar_conciliacao_att_interno",
        python_callable=gerar_conciliacao_att_interno,
    )
