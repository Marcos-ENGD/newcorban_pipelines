from __future__ import annotations

import json
import os
import time
import unicodedata
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator


DAG_ID = "newcorban_corrige_status_zero"
BASE_URL = "https://developers.newcorban.com.br/v1/proposals"
PER_PAGE = 50
STATUS_ORIGEM = 0
STATUS_DESTINO = 9107
SUBSTATUS_DESTINO = "Aguardando Formaliza\u00e7\u00e3o"
FRANQUIA_BLOQUEADA = "QUALICONSIG (INTERNO)"
TIPO_BLOQUEADO = "Crédito do Trabalhador"
REQUEST_TIMEOUT_SECONDS = 60
# Redefinido em ASCII para evitar que o acento seja corrompido no deploy.
TIPO_BLOQUEADO = "Cr\u00e9dito do Trabalhador"
MIN_UPDATE_DELAY_SECONDS = 1.5
MAX_RETRIES = 5
BANCO_OBRIGATORIO = "Quali Banking"
BANK_STATUS_OBRIGATORIO = "PENDENTE FORMALIZACAO"


def normalize_text(value) -> str:
    if value is None:
        return ""
    text = unicodedata.normalize("NFKD", str(value).strip())
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.casefold().split())


def get_token() -> str:
    token = 'nc_live_PsS9B39OC4kk2UoPShOCiksMOM8C5QwNbsUJFleH'
    if not token:
        raise ValueError("Token NEWCORBAN_API nao encontrado no ambiente.")
    return token


def response_body(response: requests.Response):
    try:
        return response.json()
    except Exception:
        return response.text


def request_get(token: str, params: dict) -> dict:
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        response = requests.get(
            BASE_URL,
            headers=headers,
            params=params,
            timeout=REQUEST_TIMEOUT_SECONDS,
        )
        if response.status_code == 429:
            wait_seconds = min(float(response.headers.get("Retry-After", "30")), 60.0)
            print(f"[BUSCA] Rate limit; aguardando {wait_seconds:.1f}s.")
            time.sleep(wait_seconds)
            continue
        if response.status_code >= 500 and attempt < MAX_RETRIES:
            wait_seconds = min(2 ** attempt, 30)
            print(f"[BUSCA] HTTP {response.status_code}; nova tentativa em {wait_seconds}s.")
            time.sleep(wait_seconds)
            continue
        if not 200 <= response.status_code < 300:
            raise RuntimeError(
                f"Erro ao buscar propostas: HTTP {response.status_code} - "
                f"{json.dumps(response_body(response), ensure_ascii=False, default=str)}"
            )
        payload = response_body(response)
        if not isinstance(payload, dict):
            raise RuntimeError("A API de propostas retornou um corpo que nao e JSON objeto.")
        return payload

    raise RuntimeError("Limite de tentativas excedido ao buscar propostas.")


def proposal_status_id(item: dict):
    proposal = item.get("proposal") or {}
    status = proposal.get("status") or item.get("status") or {}
    value = status.get("id")
    if value is None:
        value = status.get("code")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def proposal_type_values(item: dict) -> list[str]:
    proposal = item.get("proposal") or {}
    product = proposal.get("product") or item.get("product") or {}
    product_type = product.get("type") or {}

    values = [product.get("name"), product.get("key")]
    if isinstance(product_type, dict):
        values.extend([product_type.get("name"), product_type.get("key")])
    elif product_type is not None:
        values.append(product_type)
    return [str(value) for value in values if value is not None]


def proposal_franchise_name(item: dict) -> str | None:
    assignment = item.get("assignment") or {}
    franchise = assignment.get("franchise") or {}
    return franchise.get("name")


def proposal_bank_name(item: dict) -> str | None:
    proposal = item.get("proposal") or {}
    bank = proposal.get("bank") or {}
    return bank.get("name")


def proposal_bank_status(item: dict) -> str | None:
    bank_reference = item.get("bank_reference") or {}
    return bank_reference.get("bank_status")


def is_worker_credit(item: dict) -> bool:
    blocked = normalize_text(TIPO_BLOQUEADO)
    return any(blocked in normalize_text(value) for value in proposal_type_values(item))


def is_internal_franchise(item: dict) -> bool:
    return normalize_text(proposal_franchise_name(item)) == normalize_text(FRANQUIA_BLOQUEADA)


def fetch_status_zero_proposals(token: str) -> list[dict]:
    proposals: dict[int, dict] = {}
    page = 1

    while True:
        params = {
            "status[]": STATUS_ORIGEM,
            "page": page,
            "per_page": PER_PAGE,
        }
        payload = request_get(token, params)
        data = payload.get("data") or []
        meta = payload.get("meta") or {}

        if not isinstance(data, list):
            raise RuntimeError("Campo 'data' da API nao e uma lista.")

        total_before = len(proposals)
        for item in data:
            if not isinstance(item, dict):
                continue
            proposal_id = item.get("id")
            try:
                proposal_id = int(proposal_id)
            except (TypeError, ValueError):
                print(f"[BUSCA] Item sem id valido ignorado: {proposal_id!r}")
                continue
            proposals[proposal_id] = item

        last_page = meta.get("last_page")
        current_page = meta.get("current_page", page)
        print(
            f"[BUSCA] pagina={page} recebidos={len(data)} "
            f"acumulados={len(proposals)} last_page={last_page}"
        )

        has_valid_last_page = False
        if last_page is not None:
            try:
                has_valid_last_page = True
                if int(current_page) >= int(last_page):
                    break
            except (TypeError, ValueError):
                has_valid_last_page = False
        if not has_valid_last_page and len(data) < PER_PAGE:
            break
        if len(proposals) == total_before:
            raise RuntimeError(
                "A paginacao nao trouxe novos IDs; interrompida para evitar loop infinito."
            )
        page += 1

    return list(proposals.values())


def update_status(token: str, proposal_id: int) -> dict:
    endpoint = f"{BASE_URL}/{proposal_id}/status"
    payload = {
        "status_id": STATUS_DESTINO,
        "substatus": SUBSTATUS_DESTINO,
    }

    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(
                f"[ATUALIZACAO] id={proposal_id} payload="
                f"{json.dumps(payload, ensure_ascii=False)}"
            )
            response = requests.put(
                endpoint,
                headers=headers,
                json=payload,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
        except requests.RequestException as exc:
            if attempt == MAX_RETRIES:
                return {"success": False, "status_code": None, "error": str(exc)}
            wait_seconds = min(2 ** attempt, 30)
            print(f"[ATUALIZACAO] id={proposal_id} erro={exc}; retry em {wait_seconds}s.")
            time.sleep(wait_seconds)
            continue

        body = response_body(response)
        if response.status_code == 429 and attempt < MAX_RETRIES:
            wait_seconds = min(float(response.headers.get("Retry-After", "30")), 60.0)
            print(f"[ATUALIZACAO] id={proposal_id} rate limit; retry em {wait_seconds:.1f}s.")
            time.sleep(wait_seconds)
            continue
        if response.status_code >= 500 and attempt < MAX_RETRIES:
            wait_seconds = min(2 ** attempt, 30)
            print(
                f"[ATUALIZACAO] id={proposal_id} HTTP {response.status_code}; "
                f"retry em {wait_seconds}s."
            )
            time.sleep(wait_seconds)
            continue

        success = 200 <= response.status_code < 300
        return {
            "success": success,
            "status_code": response.status_code,
            "error": None if success else body,
        }

    return {"success": False, "status_code": None, "error": "Tentativas excedidas"}


def process_status_zero(**context):
    token = get_token()
    proposals = fetch_status_zero_proposals(token)

    eligible = []
    skipped_wrong_status = 0
    skipped_worker_credit = 0
    skipped_internal_franchise = 0
    skipped_wrong_bank = 0
    skipped_wrong_bank_status = 0

    for item in proposals:
        proposal_id = item.get("id")
        status_id = proposal_status_id(item)

        # Barreira de seguranca: o filtro remoto nunca e considerado suficiente.
        if status_id != STATUS_ORIGEM:
            skipped_wrong_status += 1
            print(f"[IGNORADA] id={proposal_id} status_retornado={status_id!r}")
            continue
        if is_worker_credit(item):
            skipped_worker_credit += 1
            print(
                f"[IGNORADA] id={proposal_id} tipo_bloqueado={proposal_type_values(item)!r}"
            )
            continue
        if is_internal_franchise(item):
            skipped_internal_franchise += 1
            print(
                f"[IGNORADA] id={proposal_id} "
                f"franquia_bloqueada={proposal_franchise_name(item)!r}"
            )
            continue
        if normalize_text(proposal_bank_name(item)) != normalize_text(BANCO_OBRIGATORIO):
            skipped_wrong_bank += 1
            print(
                f"[IGNORADA] id={proposal_id} "
                f"banco_invalido={proposal_bank_name(item)!r}"
            )
            continue
        if normalize_text(proposal_bank_status(item)) != normalize_text(BANK_STATUS_OBRIGATORIO):
            skipped_wrong_bank_status += 1
            print(
                f"[IGNORADA] id={proposal_id} "
                f"bank_status_invalido={proposal_bank_status(item)!r}"
            )
            continue
        eligible.append(item)
        print(
            f"[ELEGIVEL] id={proposal_id} tipos={proposal_type_values(item)!r} "
            f"franquia={proposal_franchise_name(item)!r} "
            f"banco={proposal_bank_name(item)!r} "
            f"bank_status={proposal_bank_status(item)!r}"
        )

    print(
        "[RESUMO FILTRO] "
        f"retornadas_api={len(proposals)} vao_atualizar={len(eligible)} "
        f"status_divergente={skipped_wrong_status} "
        f"credito_trabalhador={skipped_worker_credit} "
        f"franquia_interna={skipped_internal_franchise} "
        f"banco_diferente={skipped_wrong_bank} "
        f"bank_status_diferente={skipped_wrong_bank_status} "
        "execucao_automatica=True"
    )

    successes = 0
    failures = 0
    for position, item in enumerate(eligible, start=1):
        if position > 1:
            time.sleep(MIN_UPDATE_DELAY_SECONDS)
        proposal_id = int(item["id"])
        result = update_status(token, proposal_id)
        if result["success"]:
            successes += 1
        else:
            failures += 1
        print(
            f"[ATUALIZACAO] {position}/{len(eligible)} id={proposal_id} "
            f"destino={STATUS_DESTINO} status_code={result['status_code']} "
            f"sucesso={result['success']} erro={result['error']}"
        )

    return {
        "retornadas_api": len(proposals),
        "vao_atualizar": len(eligible),
        "retornadas": len(proposals),
        "elegiveis": len(eligible),
        "ignoradas_status_divergente": skipped_wrong_status,
        "ignoradas_credito_trabalhador": skipped_worker_credit,
        "ignoradas_franquia_interna": skipped_internal_franchise,
        "ignoradas_banco_diferente": skipped_wrong_bank,
        "ignoradas_bank_status_diferente": skipped_wrong_bank_status,
        "atualizadas": successes,
        "falhas": failures,
    }


default_args = {
    "owner": "qualiconsig",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=datetime(2026, 9, 22),
    schedule_interval="*/3 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["newcorban", "propostas", "correcao", "status-zero"],
) as dag:
    corrigir_status_zero = PythonOperator(
        task_id="corrigir_status_zero",
        python_callable=process_status_zero,
        max_active_tis_per_dag=1,
    )
