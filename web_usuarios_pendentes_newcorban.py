import os
import re
import json
import unicodedata
from typing import Any

import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv
from flask import Flask, flash, redirect, render_template_string, request, url_for


load_dotenv()

PORCHE_DB_CONFIG = {
    "database": os.getenv("NEWCORBAN_DB_NAME", "new_corban"),
    "user": os.getenv("NEWCORBAN_DB_USER", "porshe"),
    "password": os.getenv("NEWCORBAN_DB_PASSWORD", "p04r%XE%"),
    "host": os.getenv("NEWCORBAN_DB_HOST", "192.168.5.20"),
    "port": os.getenv("NEWCORBAN_DB_PORT", "5432"),
}

APP_DB_CONFIG = {
    "database": os.getenv("NEWCORBAN_APP_DB_NAME", "db_newcorban"),
    "user": os.getenv("NEWCORBAN_APP_DB_USER", "postgres"),
    "password": os.getenv("NEWCORBAN_APP_DB_PASSWORD", "20Qu-l1C0ns1g25"),
    "host": os.getenv("NEWCORBAN_APP_DB_HOST", "192.168.5.31"),
    "port": os.getenv("NEWCORBAN_APP_DB_PORT", "5432"),
}

API_URL = "https://developers.newcorban.com.br/v1/users"
FRANCHISES_API_URL = "https://developers.newcorban.com.br/v1/franchises"
TEAMS_API_URL = "https://developers.newcorban.com.br/v1/teams"
TOKEN = os.getenv("NEWCORBAN_TOKEN") or os.getenv("NEWCORBAN_API_TOKEN") or os.getenv("NEWCORBAN_API") or "nc_live_PsS9B39OC4kk2UoPShOCiksMOM8C5QwNbsUJFleH"
PASSWORD = os.getenv("NEWCORBAN_DEFAULT_USER_PASSWORD", "Quali1234567855")
ROLE_ID = 2775
PENDING_TABLE = "newcorban_usuarios_pendentes"
SOURCE_TABLE = "newcorban_insert_parceiros"

app = Flask(__name__)
app.secret_key = os.getenv("NEWCORBAN_WEB_SECRET_KEY", "newcorban-pendentes-local")


@app.after_request
def desabilitar_cache_da_interface(response):
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


PAGE = """
<!doctype html>
<html lang="pt-BR">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Usuários Pendentes NewCorban</title>
  <style>
    :root {
      color-scheme: light;
      --bg: #f6f7f9;
      --panel: #ffffff;
      --line: #d9dee7;
      --text: #1b2430;
      --muted: #607086;
      --accent: #0f766e;
      --danger: #b42318;
    }
    * { box-sizing: border-box; }
    body {
      margin: 0;
      font-family: Arial, Helvetica, sans-serif;
      background: var(--bg);
      color: var(--text);
    }
    header {
      padding: 18px 24px;
      background: #fff;
      border-bottom: 1px solid var(--line);
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 16px;
    }
    h1 { margin: 0; font-size: 20px; }
    main { padding: 20px 24px 32px; }
    .tabs { display: flex; gap: 8px; margin-bottom: 16px; }
    .tab { padding: 9px 14px; border-radius: 6px; color: var(--muted); background: #e9edf3; text-decoration: none; font-weight: 700; }
    .tab.active { color: #fff; background: var(--accent); }
    .form-card { background: #fff; border: 1px solid var(--line); border-radius: 8px; padding: 16px; margin-bottom: 16px; }
    .form-card form { max-width: 620px; }
    .warning { color: #92400e; background: #fffbeb; border: 1px solid #fde68a; border-radius: 6px; padding: 9px; font-size: 13px; }
    details.edit-box { margin-top: 6px; }
    details.edit-box summary { color: var(--accent); cursor: pointer; font-weight: 700; }
    .edit-form { margin-top: 10px; min-width: 280px; }
    .edit-grid { display: grid; grid-template-columns: repeat(2, minmax(160px, 1fr)); gap: 8px; }
    .toolbar { display: flex; gap: 8px; align-items: end; margin-bottom: 14px; }
    .toolbar form { display: flex; flex-direction: row; min-width: 0; flex: 1; }
    .pagination { display: flex; gap: 6px; align-items: center; justify-content: center; margin-top: 16px; }
    .pagination a, .pagination span { padding: 7px 10px; border-radius: 5px; background: #fff; border: 1px solid var(--line); text-decoration: none; color: var(--text); }
    .summary {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }
    .metric {
      background: var(--panel);
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 12px 14px;
    }
    .metric strong { display: block; font-size: 22px; }
    .metric span { color: var(--muted); font-size: 13px; }
    .flash {
      margin-bottom: 12px;
      padding: 10px 12px;
      border: 1px solid var(--line);
      border-left: 4px solid var(--accent);
      background: #fff;
      border-radius: 6px;
      white-space: pre-wrap;
    }
    .flash.error { border-left-color: var(--danger); }
    table {
      width: 100%;
      border-collapse: collapse;
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 8px;
      overflow: hidden;
    }
    th, td {
      padding: 10px;
      border-bottom: 1px solid var(--line);
      vertical-align: top;
      text-align: left;
      font-size: 13px;
    }
    th {
      color: var(--muted);
      background: #f9fafb;
      font-weight: 700;
    }
    tr:last-child td { border-bottom: 0; }
    .muted { color: var(--muted); }
    .tag {
      display: inline-block;
      padding: 3px 7px;
      border-radius: 999px;
      background: #eef6f5;
      color: #115e59;
      font-size: 12px;
      font-weight: 700;
    }
    form { display: grid; gap: 8px; min-width: 330px; }
    input, select {
      width: 100%;
      border: 1px solid var(--line);
      border-radius: 6px;
      padding: 8px;
      font: inherit;
      background: #fff;
    }
    .actions {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 8px;
    }
    .team-picked {
      color: var(--muted);
      font-size: 12px;
      line-height: 1.35;
      min-height: 16px;
    }
    button, .button {
      border: 0;
      border-radius: 6px;
      padding: 8px 10px;
      background: var(--accent);
      color: #fff;
      font-weight: 700;
      cursor: pointer;
      text-decoration: none;
      text-align: center;
      font: inherit;
    }
    button.secondary { background: #475569; }
    button:disabled { opacity: .5; cursor: not-allowed; }
    .empty {
      background: #fff;
      border: 1px solid var(--line);
      border-radius: 8px;
      padding: 18px;
      color: var(--muted);
    }
    @media (max-width: 900px) {
      .summary { grid-template-columns: 1fr; }
      table, thead, tbody, th, td, tr { display: block; }
      thead { display: none; }
      tr { border-bottom: 1px solid var(--line); }
      td { border-bottom: 0; }
      td::before {
        content: attr(data-label);
        display: block;
        color: var(--muted);
        font-size: 12px;
        font-weight: 700;
        margin-bottom: 3px;
      }
      form { min-width: 0; }
      .edit-grid { grid-template-columns: 1fr; }
    }
  </style>
</head>
<body>
  <header>
    <h1>Usuários Pendentes NewCorban</h1>
    <a class="button" href="{{ url_for('index', tab=active_tab) }}">Atualizar</a>
  </header>
  <main>
    {% for category, message in messages %}
      <div class="flash {{ category }}">{{ message }}</div>
    {% endfor %}

    <nav class="tabs">
      <a class="tab {{ 'active' if active_tab == 'instrucoes' else '' }}" href="{{ url_for('index', tab='instrucoes') }}">Instruções</a>
      <a class="tab {{ 'active' if active_tab == 'usuarios' else '' }}" href="{{ url_for('index', tab='usuarios') }}">Usuários pendentes</a>
      <a class="tab {{ 'active' if active_tab == 'editar_usuarios' else '' }}" href="{{ url_for('index', tab='editar_usuarios') }}">Gerenciar usuários</a>
      <a class="tab {{ 'active' if active_tab == 'franquias' else '' }}" href="{{ url_for('index', tab='franquias') }}">Franquias</a>
      <a class="tab {{ 'active' if active_tab == 'equipes' else '' }}" href="{{ url_for('index', tab='equipes') }}">Equipes</a>
    </nav>

    {% if active_tab == 'usuarios' %}
    {% if db_error %}
      <div class="flash error">{{ db_error }}</div>
    {% else %}
      <section class="summary">
        <div class="metric"><strong>{{ pendentes|length }}</strong><span>Logins pendentes carregados</span></div>
        <div class="metric"><strong>{{ teams|length }}</strong><span>Equipes disponíveis</span></div>
        <div class="metric"><strong>{{ total_propostas }}</strong><span>Propostas bloqueadas</span></div>
      </section>

      <datalist id="teams-list">
        {% for team in teams %}
          <option
            value="{{ team.name }} | {{ team.franchise_name }} | team {{ team.id }} / franquia {{ team.franchise_id }}"
            data-team-id="{{ team.id }}"
          ></option>
        {% endfor %}
      </datalist>

      {% if pendentes %}
        <table>
          <thead>
            <tr>
              <th>Login</th>
              <th>Propostas</th>
              <th>Referências</th>
              <th>Criar usuário</th>
            </tr>
          </thead>
          <tbody>
            {% for item in pendentes %}
              <tr>
                <td data-label="Login">
                  <strong>{{ item.login }}</strong><br>
                  <span class="muted">{{ item.login_norm }}</span><br>
                  <span class="tag">{{ item.status }}</span>
                </td>
                <td data-label="Propostas">{{ item.qtd_propostas }}</td>
                <td data-label="Referências">
                  <strong>CPFs</strong>: {{ item.cpfs_text }}<br>
                  <strong>ADEs</strong>: {{ item.numeros_ade_text }}
                </td>
                <td data-label="Criar usuário">
                  <form method="post" action="{{ url_for('create_user') }}">
                    <input type="hidden" name="pending_id" value="{{ item.id }}">
                    <label>
                      Nome
                      <input name="name" value="{{ item.nome_sugerido or item.login }}" maxlength="45" required>
                    </label>
                    <label>
                      Username
                      <input name="username" value="{{ item.login }}" maxlength="45" required>
                    </label>
                    <label>
                      Equipe / franquia
                      <input
                        class="team-search"
                        name="team_search"
                        list="teams-list"
                        placeholder="Digite nome, franquia ou ID"
                        autocomplete="off"
                        required
                      >
                      <input class="team-id" type="hidden" name="team_id" required>
                      <span class="team-picked">Nenhuma equipe selecionada</span>
                    </label>
                    <div class="actions">
                      <button type="submit">Criar e liberar</button>
                      <button class="secondary" type="submit" formaction="{{ url_for('resolve_only') }}">Só liberar</button>
                    </div>
                  </form>
                </td>
              </tr>
            {% endfor %}
          </tbody>
        </table>
      {% else %}
        <div class="empty">Nenhum login pendente com status diferente de RESOLVIDO.</div>
      {% endif %}
    {% endif %}
    {% elif active_tab == 'editar_usuarios' %}
      <div class="toolbar">
        <form method="get" action="{{ url_for('index') }}">
          <input type="hidden" name="tab" value="editar_usuarios">
          <input name="q" value="{{ user_search }}" placeholder="Buscar por nome ou username" autocomplete="off">
          <button type="submit">Buscar</button>
          {% if user_search %}<a class="button secondary" href="{{ url_for('index', tab='editar_usuarios') }}">Limpar</a>{% endif %}
        </form>
        <span>{{ users_total }} usuário(s) encontrado(s)</span>
      </div>
      <datalist id="manage-teams-list">
        {% for team in teams %}<option value="{{ team.id }} | {{ team.name }} | {{ team.franchise_name or 'Matriz' }}" data-manage-team-id="{{ team.id }}"></option>{% endfor %}
      </datalist>
      <datalist id="manage-franchises-list">
        {% for franchise in franchises %}<option value="{{ franchise.id }} | {{ franchise.name }}" data-manage-franchise-id="{{ franchise.id }}"></option>{% endfor %}
      </datalist>
      {% if users %}
        <table>
          <thead><tr><th>ID</th><th>Nome</th><th>Username</th><th>Perfil</th><th>Equipe / franquia</th><th>Status</th><th>Ação</th></tr></thead>
          <tbody>
          {% for user in users %}
            <tr>
              <td>{{ user.id }}</td><td><strong>{{ user.name }}</strong></td><td>{{ user.username }}</td>
              <td>{{ user.role_name or '-' }}</td><td>{{ user.team_name or '-' }} / {{ user.franchise_name or 'Matriz' }}</td>
              <td>{{ 'Ativo' if user.active else 'Inativo' }}</td>
              <td>
                <details class="edit-box"><summary>Editar</summary>
                  <form class="edit-form" method="post" action="{{ url_for('update_user', user_id=user.id) }}">
                    <div class="edit-grid">
                      <label>Nome <input name="name" value="{{ user.name or '' }}" minlength="3" maxlength="45" required></label>
                      <label>Username <input name="username" value="{{ user.username or '' }}" minlength="3" maxlength="45" required></label>
                      <label>Equipe
                        <input class="manage-team-search" list="manage-teams-list" value="{% if user.team_id %}{{ user.team_id }} | {{ user.team_name }} | {{ user.franchise_name or 'Matriz' }}{% endif %}" placeholder="Sem equipe" autocomplete="off">
                        <input class="manage-team-id" type="hidden" name="team_id" value="{{ user.team_id or '' }}">
                      </label>
                      <label>Franquia
                        <input class="manage-franchise-search" list="manage-franchises-list" value="{% if user.franchise_id %}{{ user.franchise_id }} | {{ user.franchise_name }}{% endif %}" placeholder="Matriz" autocomplete="off">
                        <input class="manage-franchise-id" type="hidden" name="franchise_id" value="{{ user.franchise_id or '' }}">
                      </label>
                    </div>
                    <button type="submit">Salvar alterações</button>
                  </form>
                </details>
              </td>
            </tr>
          {% endfor %}
          </tbody>
        </table>
      {% else %}<div class="empty">Nenhum usuário encontrado.</div>{% endif %}
      {% if users_pages > 1 %}
        <nav class="pagination" aria-label="Paginação de usuários">
          {% if user_page > 1 %}<a href="{{ url_for('index', tab='editar_usuarios', q=user_search, page=user_page - 1) }}">Anterior</a>{% endif %}
          <span>Página {{ user_page }} de {{ users_pages }}</span>
          {% if user_page < users_pages %}<a href="{{ url_for('index', tab='editar_usuarios', q=user_search, page=user_page + 1) }}">Próxima</a>{% endif %}
        </nav>
      {% endif %}
    {% elif active_tab == 'instrucoes' %}
      <section class="form-card">
        <h2>Como organizar parceiros, franquias e equipes</h2>
        <p>Na tela de cadastro de usuários, as opções exibidas para vinculação são as <strong>equipes</strong>, que representam as lojas.</p>
        <p>Cada equipe deve estar vinculada a uma <strong>franquia</strong>, que representa a matriz. Ao escolher uma equipe para o usuário, a franquia correspondente é vinculada por consequência.</p>
      </section>
      <section class="form-card">
        <h2>Ordem correta para novos parceiros</h2>
        <ol>
          <li>Crie a <strong>franquia</strong> do parceiro na aba Franquias.</li>
          <li>Crie a <strong>equipe</strong> na aba Equipes e vincule-a à franquia criada.</li>
          <li>Volte à aba Usuários pendentes e selecione essa equipe ao cadastrar o usuário.</li>
        </ol>
        <div class="warning"><strong>Importante:</strong> mesmo que o parceiro não possua lojas abaixo dele, deve ser criada uma equipe padrão vinculada à franquia matriz do parceiro. Todo usuário precisa ser associado a uma equipe.</div>
      </section>
    {% elif active_tab == 'equipes' %}
      {% if db_error %}
        <div class="flash error">{{ db_error }}</div>
      {% else %}
        <section class="form-card">
          <h2>Criar equipe</h2>
          <form method="post" action="{{ url_for('create_team') }}" class="team-create-form">
            <label>Nome <input name="name" minlength="2" maxlength="50" required></label>
                         <div class="warning">Tente manter o mesmo nome do ADONIS.</div>

            <label>
              Franquia
              <input class="franchise-search" list="franchises-list" placeholder="Pesquise pelo nome ou ID; vazio = Matriz" autocomplete="off">
              <input class="franchise-id" type="hidden" name="franchise_id">
              <span class="franchise-picked team-picked">Matriz (sem franquia)</span>
              <datalist id="franchises-list">
                {% for franchise in franchises %}
                  <option value="{{ franchise.name }} | franquia {{ franchise.id }}" data-franchise-id="{{ franchise.id }}"></option>
                {% endfor %}
              </datalist>
            </label>
            <button type="submit">Criar equipe</button>
          </form>
        </section>

        {% if teams %}
          <table>
            <thead><tr><th>ID</th><th>Nome</th><th>Franquia</th><th>Criada em</th><th>Ação</th></tr></thead>
            <tbody>
              {% for team in teams %}
                <tr>
                  <td data-label="ID">{{ team.id }}</td>
                  <td data-label="Nome"><strong>{{ team.name }}</strong></td>
                  <td data-label="Franquia">{{ team.franchise_name or 'Matriz' }}{% if team.franchise_id %} ({{ team.franchise_id }}){% endif %}</td>
                  <td data-label="Criada em">{{ team.created_at or '-' }}</td>
                  <td><details class="edit-box"><summary>Editar</summary>
                    <form class="edit-form" method="post" action="{{ url_for('update_team', team_id=team.id) }}">
                      <label>Nome <input name="name" value="{{ team.name }}" minlength="2" maxlength="50" required></label>
                      <label>Franquia <select name="franchise_id"><option value="">Matriz</option>{% for franchise in franchises %}<option value="{{ franchise.id }}" {{ 'selected' if franchise.id == team.franchise_id else '' }}>{{ franchise.name }} ({{ franchise.id }})</option>{% endfor %}</select></label>
                      <button type="submit">Salvar alterações</button>
                    </form>
                  </details></td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        {% else %}
          <div class="empty">Nenhuma equipe encontrada.</div>
        {% endif %}
      {% endif %}
    {% else %}
      {% if db_error %}
        <div class="flash error">{{ db_error }}</div>
      {% else %}
        <section class="form-card">
          <h2>Criar franquia</h2>
          <form method="post" action="{{ url_for('create_franchise') }}">
            <label>Nome <input name="name" maxlength="255" required></label>
         <div class="warning">Exemplo: PARCEIRO - VENDAS QUALI</div>
            <label>
              CPF ou CNPJ
              <input name="tax_id" inputmode="numeric" placeholder="Opcional">
            </label>
            <div class="warning">O ideal é informar CPF ou CNPJ. Caso vazio, sera enviado como nulo.</div>
            <label>Telefone <input name="phone" inputmode="tel" placeholder="Opcional"></label>
            <button type="submit">Criar franquia</button>
          </form>
        </section>

        {% if franchises %}
          <table>
            <thead><tr><th>ID</th><th>Nome</th><th>Criada em</th><th>Ação</th></tr></thead>
            <tbody>
              {% for franchise in franchises %}
                <tr>
                  <td data-label="ID">{{ franchise.id }}</td>
                  <td data-label="Nome"><strong>{{ franchise.name }}</strong></td>
                  <td data-label="Criada em">{{ franchise.created_at or '-' }}</td>
                  <td><details class="edit-box"><summary>Editar</summary>
                    <form class="edit-form" method="post" action="{{ url_for('update_franchise', franchise_id=franchise.id) }}">
                      <label>Nome <input name="name" value="{{ franchise.name or '' }}" minlength="2" maxlength="120" required></label>
                      <button type="submit">Salvar alterações</button>
                    </form>
                  </details></td>
                </tr>
              {% endfor %}
            </tbody>
          </table>
        {% else %}
          <div class="empty">Nenhuma franquia encontrada.</div>
        {% endif %}
      {% endif %}
    {% endif %}
  </main>
  <script>
    window.addEventListener("pageshow", (event) => {
      if (event.persisted) {
        window.location.reload();
      }
    });

    const teamOptions = Array.from(document.querySelectorAll("#teams-list option")).map((option) => ({
      label: option.value,
      id: option.dataset.teamId
    }));

    function applyTeamSearch(input) {
      const form = input.closest("form");
      const hidden = form.querySelector(".team-id");
      const picked = form.querySelector(".team-picked");
      const typed = input.value.trim().toLowerCase();
      const selected = teamOptions.find((team) => team.label.toLowerCase() === typed);

      hidden.value = selected ? selected.id : "";
      picked.textContent = selected ? `Selecionada: ${selected.label}` : "Nenhuma equipe selecionada";
    }

    document.querySelectorAll(".team-search").forEach((input) => {
      input.addEventListener("input", () => applyTeamSearch(input));
      input.addEventListener("change", () => applyTeamSearch(input));
      input.closest("form").addEventListener("submit", (event) => {
        applyTeamSearch(input);
        if (!input.closest("form").querySelector(".team-id").value) {
          event.preventDefault();
          input.setCustomValidity("Selecione uma equipe da lista.");
          input.reportValidity();
        } else {
          input.setCustomValidity("");
        }
      });
    });

    const franchiseOptions = Array.from(document.querySelectorAll("#franchises-list option")).map((option) => ({
      label: option.value,
      id: option.dataset.franchiseId
    }));
    document.querySelectorAll(".franchise-search").forEach((input) => {
      const applyFranchise = () => {
        const form = input.closest("form");
        const hidden = form.querySelector(".franchise-id");
        const picked = form.querySelector(".franchise-picked");
        const typed = input.value.trim().toLowerCase();
        const selected = franchiseOptions.find((item) => item.label.toLowerCase() === typed);
        hidden.value = selected ? selected.id : "";
        picked.textContent = selected ? `Selecionada: ${selected.label}` : "Matriz (sem franquia)";
      };
      input.addEventListener("input", applyFranchise);
      input.addEventListener("change", applyFranchise);
      input.closest("form").addEventListener("submit", (event) => {
        applyFranchise();
        if (input.value.trim() && !input.closest("form").querySelector(".franchise-id").value) {
          event.preventDefault();
          input.setCustomValidity("Selecione uma franquia da lista ou deixe vazio para Matriz.");
          input.reportValidity();
        } else {
          input.setCustomValidity("");
        }
      });
    });

    function bindManageLookup(inputSelector, optionSelector, hiddenSelector, dataKey, invalidMessage) {
      const options = Array.from(document.querySelectorAll(optionSelector)).map((option) => ({
        label: option.value.toLowerCase(),
        id: option.dataset[dataKey]
      }));
      document.querySelectorAll(inputSelector).forEach((input) => {
        const form = input.closest("form");
        const hidden = form.querySelector(hiddenSelector);
        const applyValue = () => {
          const typed = input.value.trim();
          if (!typed) {
            hidden.value = "";
            input.setCustomValidity("");
            return true;
          }
          const selected = options.find((item) => item.label === typed.toLowerCase());
          hidden.value = selected ? selected.id : "";
          input.setCustomValidity(selected ? "" : invalidMessage);
          return Boolean(selected);
        };
        input.addEventListener("input", applyValue);
        input.addEventListener("change", applyValue);
        form.addEventListener("submit", (event) => {
          if (!applyValue()) {
            event.preventDefault();
            input.reportValidity();
          }
        });
      });
    }

    bindManageLookup(
      ".manage-team-search", "#manage-teams-list option", ".manage-team-id",
      "manageTeamId", "Selecione uma equipe da lista ou deixe vazio."
    );
    bindManageLookup(
      ".manage-franchise-search", "#manage-franchises-list option", ".manage-franchise-id",
      "manageFranchiseId", "Selecione uma franquia da lista ou deixe vazio para Matriz."
    );
  </script>
</body>
</html>
"""


def get_app_conn():
    return psycopg2.connect(**APP_DB_CONFIG)


def get_porche_conn():
    return psycopg2.connect(**PORCHE_DB_CONFIG)


def normalize_login(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    text = "".join(
        ch for ch in unicodedata.normalize("NFD", text)
        if unicodedata.category(ch) != "Mn"
    )
    text = re.sub(r"[^a-z0-9]", "", text)
    return text or None


def response_to_json(response: requests.Response) -> Any:
    try:
        return response.json()
    except ValueError:
        return response.text


def as_text_list(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, list):
        return ", ".join(str(item) for item in value[:8]) or "-"
    return str(value)


def fetch_pendentes(conn) -> list[dict[str, Any]]:
    sql = f"""
        SELECT id, login, login_norm, nome_sugerido, qtd_propostas, cpfs, numeros_ade, status
        FROM public.{PENDING_TABLE}
        WHERE COALESCE(upper(trim(status)), '') <> 'RESOLVIDO'
        ORDER BY qtd_propostas DESC, updated_at DESC
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        rows = [dict(row) for row in cur.fetchall()]
    for row in rows:
        row["cpfs_text"] = as_text_list(row.get("cpfs"))
        row["numeros_ade_text"] = as_text_list(row.get("numeros_ade"))
    return rows


def fetch_teams(conn) -> list[dict[str, Any]]:
    sql = """
        SELECT id, name, franchise_id, franchise_name, created_at
        FROM public.teams
        ORDER BY franchise_name NULLS LAST, name
    """
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql)
        return [dict(row) for row in cur.fetchall()]


def fetch_franchises(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT id, name, created_at
            FROM public.franchises
            ORDER BY name
            """
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_users(
    conn, search: str = "", limit: int = 25, offset: int = 0
) -> tuple[list[dict[str, Any]], int]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        where_sql = ""
        params: list[Any] = []
        if search:
            where_sql = "WHERE name ILIKE %s OR username ILIKE %s"
            pattern = f"%{search}%"
            params.extend([pattern, pattern])
        cur.execute(f"SELECT COUNT(*) AS total FROM public.users {where_sql}", params)
        total = int(cur.fetchone()["total"])
        cur.execute(
            f"""
            SELECT id, name, username, role_id, role_name, team_id, team_name,
                   franchise_id, franchise_name, active, is_support
            FROM public.users
            {where_sql}
            ORDER BY name
            LIMIT %s OFFSET %s
            """,
            [*params, limit, offset],
        )
        return [dict(row) for row in cur.fetchall()], total


def fetch_roles(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT DISTINCT role_id AS id, role_name AS name
            FROM public.users
            WHERE role_id IS NOT NULL AND role_name IS NOT NULL
            ORDER BY role_name
            """
        )
        return [dict(row) for row in cur.fetchall()]


def fetch_pending(conn, pending_id: int) -> dict[str, Any] | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"SELECT * FROM public.{PENDING_TABLE} WHERE id = %s",
            (pending_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def fetch_team(conn, team_id: int) -> dict[str, Any] | None:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            "SELECT id, name, franchise_id, franchise_name FROM public.teams WHERE id = %s",
            (team_id,),
        )
        row = cur.fetchone()
    return dict(row) if row else None


def unlock_source_rows(conn, login_norm: str) -> int:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            f"""
            SELECT "numeroAde", login
            FROM public.{SOURCE_TABLE}
            WHERE bloqueio_envio = 'USUARIO_NAO_ENCONTRADO'
            """
        )
        rows = [dict(row) for row in cur.fetchall()]

    numeros_ade = [
        row["numeroAde"]
        for row in rows
        if normalize_login(row.get("login")) == login_norm and row.get("numeroAde")
    ]
    if not numeros_ade:
        return 0

    with conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE public.{SOURCE_TABLE}
            SET bloqueio_envio = NULL,
                bloqueio_envio_em = NULL,
                processada_erro = NULL,
                updated_at = now()
            WHERE "numeroAde" = ANY(%s)
            """,
            (numeros_ade,),
        )
        return cur.rowcount


def mark_resolved(pending_conn, source_conn, pending_id: int, user_id: int | None, team_id: int | None, franchise_id: int | None) -> tuple[int, str | None]:
    pending = fetch_pending(pending_conn, pending_id)
    if not pending:
        raise ValueError("Login pendente nao encontrado.")

    login_norm = pending["login_norm"]
    with pending_conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE public.{PENDING_TABLE}
            SET status = 'RESOLVIDO',
                usuario_newcorban_id = COALESCE(%s, usuario_newcorban_id),
                team_id = COALESCE(%s, team_id),
                franchise_id = COALESCE(%s, franchise_id),
                resolvido_em = now(),
                updated_at = now()
            WHERE id = %s
            """,
            (user_id, team_id, franchise_id, pending_id),
        )

    try:
        unlocked = unlock_source_rows(source_conn, login_norm)
        return unlocked, None
    except Exception as exc:
        source_conn.rollback()
        return 0, f"Usuario marcado como RESOLVIDO, mas nao consegui destravar propostas: {exc}"


def build_payload(name: str, username: str, team: dict[str, Any]) -> dict[str, Any]:
    return {
        "name": name[:45],
        "username": username[:45],
        "cpf": None,
        "password": PASSWORD,
        "role_id": ROLE_ID,
        "team_id": int(team["id"]),
        "franchise_id": int(team["franchise_id"]) if team.get("franchise_id") is not None else None,
        "active": False,
        "is_hidden": False,
        "is_bot": False,
        "two_factor_required": False,
    }


def extract_user_id(api_response: Any) -> int | None:
    if isinstance(api_response, dict):
        for path in (("id",), ("user", "id"), ("data", "id")):
            current = api_response
            for key in path:
                current = current.get(key) if isinstance(current, dict) else None
            if current is not None:
                try:
                    return int(current)
                except (TypeError, ValueError):
                    return None
    return None


def nested_value(data: dict[str, Any], *paths: tuple[str, ...]) -> Any:
    for path in paths:
        current: Any = data
        for key in path:
            current = current.get(key) if isinstance(current, dict) else None
        if current is not None:
            return current
    return None


def unwrap_user_response(api_response: Any) -> dict[str, Any]:
    if not isinstance(api_response, dict):
        raise ValueError(f"Resposta inesperada ao consultar usuario: {api_response}")
    for path in (("data", "user"), ("data",), ("user",)):
        current: Any = api_response
        for key in path:
            current = current.get(key) if isinstance(current, dict) else None
        if isinstance(current, dict) and current.get("id") is not None:
            return current
    if api_response.get("id") is not None:
        return api_response
    raise ValueError(f"GET do usuario nao retornou um objeto com id: {api_response}")


def buscar_usuario_api(user_id: int, headers: dict[str, str]) -> dict[str, Any]:
    response = requests.get(f"{API_URL}/{user_id}", headers=headers, timeout=60)
    api_response = response_to_json(response)
    if not 200 <= response.status_code < 300:
        raise RuntimeError(
            f"Erro ao consultar usuario {user_id}. Status {response.status_code}. Resposta: {api_response}"
        )
    return unwrap_user_response(api_response)


def upsert_usuario_local(conn, user: dict[str, Any]) -> None:
    user_id = nested_value(user, ("id",))
    if user_id is None:
        raise ValueError("Nao foi possivel sincronizar usuario sem id.")

    valores = {
        "id": int(user_id),
        "name": nested_value(user, ("name",)),
        "username": nested_value(user, ("username",)),
        "role_id": nested_value(user, ("role_id",), ("role", "id")),
        "role_name": nested_value(user, ("role_name",), ("role", "name")),
        "team_id": nested_value(user, ("team_id",), ("team", "id")),
        "team_name": nested_value(user, ("team_name",), ("team", "name")),
        "franchise_id": nested_value(user, ("franchise_id",), ("franchise", "id"), ("team", "franchise_id")),
        "franchise_name": nested_value(user, ("franchise_name",), ("franchise", "name"), ("team", "franchise_name")),
        "active": nested_value(user, ("active",)),
        "is_hidden": nested_value(user, ("is_hidden",)),
        "is_bot": nested_value(user, ("is_bot",)),
        "is_support": nested_value(user, ("is_support",)),
        "two_factor_required": nested_value(user, ("two_factor_required",)),
        "two_factor_enabled": nested_value(user, ("two_factor_enabled",)),
        "online": nested_value(user, ("online",)),
        "avatar_url": nested_value(user, ("avatar_url",)),
        "created_at": nested_value(user, ("created_at",)),
        "deleted_at": nested_value(user, ("deleted_at",)),
        "last_login_at": nested_value(user, ("last_login_at",)),
        "last_activity_at": nested_value(user, ("last_activity_at",)),
    }
    colunas = list(valores)
    placeholders = ", ".join(["%s"] * len(colunas))
    updates = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in colunas if col != "id")
    sql = f'''
        INSERT INTO public.users ({", ".join(f'"{col}"' for col in colunas)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {updates}
    '''
    with conn.cursor() as cur:
        cur.execute(sql, [valores[col] for col in colunas])


def unwrap_franchise_response(api_response: Any) -> dict[str, Any]:
    if not isinstance(api_response, dict):
        raise ValueError(f"Resposta inesperada da franquia: {api_response}")
    for path in (("data", "franchise"), ("data",), ("franchise",)):
        current: Any = api_response
        for key in path:
            current = current.get(key) if isinstance(current, dict) else None
        if isinstance(current, dict) and current.get("id") is not None:
            return current
    if api_response.get("id") is not None:
        return api_response
    raise ValueError(f"Resposta da franquia nao contem id: {api_response}")


def buscar_franquia_api(franchise_id: int, headers: dict[str, str]) -> dict[str, Any]:
    response = requests.get(f"{FRANCHISES_API_URL}/{franchise_id}", headers=headers, timeout=60)
    api_response = response_to_json(response)
    if not 200 <= response.status_code < 300:
        raise RuntimeError(
            f"Erro ao consultar franquia {franchise_id}. Status {response.status_code}. Resposta: {api_response}"
        )
    return unwrap_franchise_response(api_response)


def upsert_franquia_local(conn, franchise: dict[str, Any]) -> None:
    valores = {
        "id": nested_value(franchise, ("id",)),
        "name": nested_value(franchise, ("name",)),
        "tax_id": nested_value(franchise, ("tax_id",)),
        "email": nested_value(franchise, ("email",)),
        "phone": nested_value(franchise, ("phone",)),
        "phone_type": nested_value(franchise, ("phone_type",)),
        "postal_code": nested_value(franchise, ("postal_code",)),
        "street": nested_value(franchise, ("street",)),
        "street_number": nested_value(franchise, ("street_number",)),
        "neighborhood": nested_value(franchise, ("neighborhood",)),
        "state": nested_value(franchise, ("state",)),
        "city": nested_value(franchise, ("city",)),
        "notes": nested_value(franchise, ("notes",)),
        "active": nested_value(franchise, ("active",)),
        "created_by": nested_value(franchise, ("created_by",), ("creator", "id")),
        "created_at": nested_value(franchise, ("created_at",)),
        "deleted_at": nested_value(franchise, ("deleted_at",)),
    }
    if valores["id"] is None:
        raise ValueError("Nao foi possivel sincronizar franquia sem id.")
    valores["id"] = int(valores["id"])
    colunas = list(valores)
    placeholders = ", ".join(["%s"] * len(colunas))
    updates = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in colunas if col != "id")
    sql = f'''
        INSERT INTO public.franchises ({", ".join(f'"{col}"' for col in colunas)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {updates}
    '''
    with conn.cursor() as cur:
        cur.execute(sql, [valores[col] for col in colunas])


def unwrap_team_response(api_response: Any) -> dict[str, Any]:
    if not isinstance(api_response, dict):
        raise ValueError(f"Resposta inesperada da equipe: {api_response}")
    for path in (("data", "team"), ("data",), ("team",)):
        current: Any = api_response
        for key in path:
            current = current.get(key) if isinstance(current, dict) else None
        if isinstance(current, dict) and current.get("id") is not None:
            return current
    if api_response.get("id") is not None:
        return api_response
    raise ValueError(f"Resposta da equipe nao contem id: {api_response}")


def buscar_equipe_api(team_id: int, headers: dict[str, str]) -> dict[str, Any]:
    response = requests.get(f"{TEAMS_API_URL}/{team_id}", headers=headers, timeout=60)
    api_response = response_to_json(response)
    if not 200 <= response.status_code < 300:
        raise RuntimeError(
            f"Erro ao consultar equipe {team_id}. Status {response.status_code}. Resposta: {api_response}"
        )
    return unwrap_team_response(api_response)


def upsert_equipe_local(conn, team: dict[str, Any]) -> None:
    valores = {
        "id": nested_value(team, ("id",)),
        "name": nested_value(team, ("name",)),
        "franchise_id": nested_value(team, ("franchise_id",), ("franchise", "id")),
        "franchise_name": nested_value(team, ("franchise_name",), ("franchise", "name")),
        "user_count": nested_value(team, ("user_count",)),
        "can_manage": nested_value(team, ("can_manage",)),
        "created_at": nested_value(team, ("created_at",)),
    }
    if valores["id"] is None:
        raise ValueError("Nao foi possivel sincronizar equipe sem id.")
    valores["id"] = int(valores["id"])
    if valores["user_count"] is None:
        valores["user_count"] = 0
    if valores["can_manage"] is None:
        valores["can_manage"] = True
    if valores["franchise_id"] is not None and valores["franchise_name"] is None:
        with conn.cursor() as cur:
            cur.execute("SELECT name FROM public.franchises WHERE id = %s", (valores["franchise_id"],))
            franchise_row = cur.fetchone()
        valores["franchise_name"] = franchise_row[0] if franchise_row else None
    # Se a API nao devolver a data, nao envie NULL: deixa o default do banco
    # preencher no INSERT e preserva o valor existente no UPDATE.
    if valores["created_at"] is None:
        valores.pop("created_at")
    colunas = list(valores)
    placeholders = ", ".join(["%s"] * len(colunas))
    updates = ", ".join(f'"{col}" = EXCLUDED."{col}"' for col in colunas if col != "id")
    sql = f'''
        INSERT INTO public.teams ({", ".join(f'"{col}"' for col in colunas)})
        VALUES ({placeholders})
        ON CONFLICT (id) DO UPDATE SET {updates}
    '''
    with conn.cursor() as cur:
        cur.execute(sql, [valores[col] for col in colunas])


def atualizar_equipe_local(conn, team_id: int, name: str, franchise_id: int | None) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE public.teams
            SET name = %s,
                franchise_id = %s,
                franchise_name = (SELECT name FROM public.franchises WHERE id = %s)
            WHERE id = %s
            """,
            (name, franchise_id, franchise_id, team_id),
        )


def atualizar_franquia_local(conn, franchise_id: int, payload: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE public.franchises SET name = %s WHERE id = %s", (payload["name"], franchise_id))


def usuario_ja_existe(status_code: int, api_response: Any) -> bool:
    if status_code not in {400, 409, 422}:
        return False
    text = str(api_response).lower()
    return any(term in text for term in ("already", "existe", "unique", "duplic", "username"))


def headers_api() -> dict[str, str]:
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }


def put_api(endpoint: str, payload: dict[str, Any]) -> tuple[requests.Response, Any]:
    response = requests.put(endpoint, headers=headers_api(), json=payload, timeout=60)
    return response, response_to_json(response)


@app.route("/")
def index():
    active_tab = request.args.get("tab", "usuarios")
    if active_tab not in {"usuarios", "editar_usuarios", "instrucoes", "franquias", "equipes"}:
        active_tab = "usuarios"
    db_error = None
    pendentes = []
    teams = []
    franchises = []
    users = []
    roles = []
    user_search = request.args.get("q", "").strip()
    try:
        user_page = max(1, int(request.args.get("page", "1")))
    except ValueError:
        user_page = 1
    users_per_page = 25
    users_total = 0
    users_pages = 1
    try:
        if active_tab == "instrucoes":
            pass
        else:
            with get_app_conn() as app_conn, get_porche_conn() as porche_conn:
                if active_tab == "usuarios":
                    pendentes = fetch_pendentes(app_conn)
                    teams = fetch_teams(porche_conn)
                elif active_tab == "editar_usuarios":
                    users, users_total = fetch_users(
                        porche_conn,
                        search=user_search,
                        limit=users_per_page,
                        offset=(user_page - 1) * users_per_page,
                    )
                    users_pages = max(1, (users_total + users_per_page - 1) // users_per_page)
                    teams = fetch_teams(porche_conn)
                    franchises = fetch_franchises(porche_conn)
                elif active_tab == "franquias":
                    franchises = fetch_franchises(porche_conn)
                else:
                    teams = fetch_teams(porche_conn)
                    franchises = fetch_franchises(porche_conn)
    except Exception as exc:
        db_error = str(exc)

    total_propostas = sum(int(item.get("qtd_propostas") or 0) for item in pendentes)
    return render_template_string(
        PAGE,
        pendentes=pendentes,
        teams=teams,
        franchises=franchises,
        users=users,
        roles=roles,
        user_search=user_search,
        user_page=user_page,
        users_total=users_total,
        users_pages=users_pages,
        active_tab=active_tab,
        total_propostas=total_propostas,
        db_error=db_error,
        messages=list(get_flashed_messages_with_categories()),
    )


def get_flashed_messages_with_categories():
    from flask import get_flashed_messages

    return get_flashed_messages(with_categories=True)


@app.post("/usuarios/<int:user_id>/editar")
def update_user(user_id: int):
    payload = {
        "name": request.form.get("name", "").strip(),
        "username": request.form.get("username", "").strip(),
        "team_id": int(request.form["team_id"]) if request.form.get("team_id") else None,
        "franchise_id": int(request.form["franchise_id"]) if request.form.get("franchise_id") else None,
    }
    try:
        response, api_response = put_api(f"{API_URL}/{user_id}", payload)
        if not 200 <= response.status_code < 300:
            flash(f"Erro ao atualizar usuario {user_id}. Status {response.status_code}: {api_response}", "error")
        else:
            user_complete = buscar_usuario_api(user_id, headers_api())
            with get_porche_conn() as conn:
                upsert_usuario_local(conn, user_complete)
                conn.commit()
            flash(f"Usuario {user_id} atualizado e sincronizado.", "success")
    except Exception as exc:
        flash(f"Erro ao atualizar usuario {user_id}: {exc}", "error")
    return redirect(url_for("index", tab="editar_usuarios"))


@app.post("/equipes/<int:team_id>/editar")
def update_team(team_id: int):
    name = request.form.get("name", "").strip()
    franchise_id = int(request.form["franchise_id"]) if request.form.get("franchise_id") else None
    payload = {"name": name, "franchise_id": franchise_id}
    try:
        response, api_response = put_api(f"{TEAMS_API_URL}/{team_id}", payload)
        if not 200 <= response.status_code < 300:
            flash(f"Erro ao atualizar equipe {team_id}. Status {response.status_code}: {api_response}", "error")
        else:
            team_response = {"id": team_id, **payload}
            if isinstance(api_response, dict):
                try:
                    team_response.update(unwrap_team_response(api_response))
                except ValueError:
                    pass
            with get_porche_conn() as conn:
                atualizar_equipe_local(conn, team_id, name, franchise_id)
                conn.commit()
            flash(f"Equipe {team_id} atualizada e sincronizada.", "success")
    except Exception as exc:
        flash(f"Erro ao atualizar equipe {team_id}: {exc}", "error")
    return redirect(url_for("index", tab="equipes"))


@app.post("/franquias/<int:franchise_id>/editar")
def update_franchise(franchise_id: int):
    payload = {"name": request.form.get("name", "").strip()}
    try:
        response, api_response = put_api(f"{FRANCHISES_API_URL}/{franchise_id}", payload)
        if not 200 <= response.status_code < 300:
            flash(f"Erro ao atualizar franquia {franchise_id}. Status {response.status_code}: {api_response}", "error")
        else:
            with get_porche_conn() as conn:
                atualizar_franquia_local(conn, franchise_id, payload)
                conn.commit()
            flash(f"Franquia {franchise_id} atualizada e sincronizada.", "success")
    except Exception as exc:
        flash(f"Erro ao atualizar franquia {franchise_id}: {exc}", "error")
    return redirect(url_for("index", tab="franquias"))


@app.post("/criar")
def create_user():
    pending_id = int(request.form["pending_id"])
    team_id_raw = request.form.get("team_id", "").strip()
    if not team_id_raw:
        team_search = request.form.get("team_search", "")
        team_match = re.search(r"\bteam\s+(\d+)\b", team_search, flags=re.IGNORECASE)
        team_id_raw = team_match.group(1) if team_match else ""
    if not team_id_raw.isdigit():
        flash("Selecione uma equipe valida na lista antes de criar o usuario.", "error")
        return redirect(url_for("index", tab="usuarios"))
    team_id = int(team_id_raw)
    name = request.form["name"].strip()
    username = request.form["username"].strip()

    try:
        with get_app_conn() as pending_conn, get_app_conn() as source_conn, get_porche_conn() as porche_conn:
            team = fetch_team(porche_conn, team_id)
            if not team:
                raise ValueError("Equipe selecionada nao encontrada.")

            payload = build_payload(name, username, team)
            headers = {
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {TOKEN}",
            }
            print(
                "\n[CRIACAO USUARIO] REQUISICAO ENVIADA:\n"
                + json.dumps(
                    {"method": "POST", "url": API_URL, "payload": payload},
                    ensure_ascii=False,
                    default=str,
                    indent=2,
                ),
                flush=True,
            )
            response = requests.post(API_URL, headers=headers, json=payload, timeout=60)
            api_response = response_to_json(response)
            print(
                "\n[CRIACAO USUARIO] RESPOSTA COMPLETA DA API:\n"
                + json.dumps(
                    {
                        "status_code": response.status_code,
                        "reason": response.reason,
                        "url": response.url,
                        "headers": dict(response.headers),
                        "body_raw": response.text,
                        "body_json": api_response,
                    },
                    ensure_ascii=False,
                    default=str,
                    indent=2,
                ),
                flush=True,
            )

            if not 200 <= response.status_code < 300 and not usuario_ja_existe(response.status_code, api_response):
                flash(f"Erro ao criar usuario {username}. Status {response.status_code}. Resposta: {api_response}", "error")
                return redirect(url_for("index"))

            user_id = extract_user_id(api_response)
            if user_id is None:
                raise ValueError(
                    f"A criacao/consulta nao retornou o id do usuario {username}; "
                    "nao foi possivel sincronizar public.users."
                )

            usuario_completo = buscar_usuario_api(user_id, headers)
            upsert_usuario_local(porche_conn, usuario_completo)
            unlocked, warning = mark_resolved(pending_conn, source_conn, pending_id, user_id, team_id, team.get("franchise_id"))
            pending_conn.commit()
            source_conn.commit()
            porche_conn.commit()
            if warning:
                flash(warning, "error")
            action = "Usuario ja existia; login liberado" if not 200 <= response.status_code < 300 else f"Usuario {username} criado e login liberado"
            flash(
                f"{action}. Usuario {user_id} sincronizado em new_corban.public.users. "
                f"Propostas desbloqueadas: {unlocked}.",
                "success",
            )
    except Exception as exc:
        flash(f"Erro: {exc}", "error")

    return redirect(url_for("index"))


@app.post("/franquias/criar")
def create_franchise():
    name = request.form.get("name", "").strip()
    tax_id = re.sub(r"\D", "", request.form.get("tax_id", "")) or "00000000000"
    phone = re.sub(r"\D", "", request.form.get("phone", "")) or None

    if not name:
        flash("Nome da franquia e obrigatorio.", "error")
        return redirect(url_for("index", tab="franquias"))

    payload: dict[str, Any] = {"name": name, "tax_id": tax_id}
    if phone:
        payload["phone"] = phone
        payload["phone_type"] = 1

    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    try:
        response = requests.post(FRANCHISES_API_URL, headers=headers, json=payload, timeout=60)
        api_response = response_to_json(response)
        if not 200 <= response.status_code < 300:
            flash(
                f"Erro ao criar franquia. Status {response.status_code}. Resposta: {api_response}",
                "error",
            )
            return redirect(url_for("index", tab="franquias"))

        franchise_created = unwrap_franchise_response(api_response)
        franchise_id = int(franchise_created["id"])
        try:
            franchise_complete = buscar_franquia_api(franchise_id, headers)
        except Exception:
            # Algumas versoes da API ja devolvem o recurso completo no POST.
            franchise_complete = {**payload, **franchise_created}

        with get_porche_conn() as porche_conn:
            upsert_franquia_local(porche_conn, franchise_complete)
            porche_conn.commit()

        flash(
            f"Franquia {name} criada com ID {franchise_id} e sincronizada em new_corban.public.franchises.",
            "success",
        )
    except Exception as exc:
        flash(f"Franquia criada/consultada com erro de sincronizacao: {exc}", "error")

    return redirect(url_for("index", tab="franquias"))


@app.post("/equipes/criar")
def create_team():
    name = request.form.get("name", "").strip()
    franchise_id_raw = request.form.get("franchise_id", "").strip()
    franchise_id = int(franchise_id_raw) if franchise_id_raw else None

    if not 2 <= len(name) <= 50:
        flash("O nome da equipe deve ter entre 2 e 50 caracteres.", "error")
        return redirect(url_for("index", tab="equipes"))

    payload = {"name": name, "franchise_id": franchise_id}
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "Authorization": f"Bearer {TOKEN}",
    }
    try:
        response = requests.post(TEAMS_API_URL, headers=headers, json=payload, timeout=60)
        api_response = response_to_json(response)
        if not 200 <= response.status_code < 300:
            flash(
                f"Erro ao criar equipe. Status {response.status_code}. Resposta: {api_response}",
                "error",
            )
            return redirect(url_for("index", tab="equipes"))

        team_created = unwrap_team_response(api_response)
        team_id = int(team_created["id"])
        # A API de equipes nao oferece GET /teams/{id}; completa o retorno do
        # POST com o payload enviado e dados locais da franquia no upsert.
        team_complete = {**payload, **team_created}

        with get_porche_conn() as porche_conn:
            upsert_equipe_local(porche_conn, team_complete)
            porche_conn.commit()

        flash(
            f"Equipe {name} criada com ID {team_id} e sincronizada em new_corban.public.teams.",
            "success",
        )
    except Exception as exc:
        flash(f"Equipe criada/consultada com erro de sincronizacao: {exc}", "error")

    return redirect(url_for("index", tab="equipes"))


@app.post("/liberar")
def resolve_only():
    pending_id = int(request.form["pending_id"])
    team_id_raw = request.form.get("team_id")
    team_id = int(team_id_raw) if team_id_raw else None

    try:
        with get_app_conn() as pending_conn, get_app_conn() as source_conn, get_porche_conn() as porche_conn:
            team = fetch_team(porche_conn, team_id) if team_id else None
            unlocked, warning = mark_resolved(
                pending_conn,
                source_conn,
                pending_id=pending_id,
                user_id=None,
                team_id=team_id,
                franchise_id=team.get("franchise_id") if team else None,
            )
            pending_conn.commit()
            source_conn.commit()
            if warning:
                flash(warning, "error")
            flash(f"Login marcado como resolvido. Propostas desbloqueadas: {unlocked}.", "success")
    except Exception as exc:
        flash(f"Erro: {exc}", "error")

    return redirect(url_for("index"))


if __name__ == "__main__":
    debug = os.getenv("NEWCORBAN_WEB_DEBUG", "false").strip().lower() in {"1", "true", "sim", "s"}
    app.run(
        host=os.getenv("NEWCORBAN_WEB_HOST", "0.0.0.0"),
        port=int(os.getenv("NEWCORBAN_WEB_PORT", "5055")),
        debug=debug,
    )
