import oracledb
import json
import time
import requests
from pathlib import Path
from datetime import datetime
import os 
from dotenv import load_dotenv

# ==========================
# ORACLE - MODO THICK
# ==========================
oracledb.init_oracle_client(lib_dir=r"C:\Oracle\instantclient_19_29")

load_dotenv()

USUARIO = os.getenv("ORACLE_USER")
SENHA = os.getenv("ORACLE_PASSWORD")
DSN = os.getenv("ORACLE_DSN")

BOT_TOKEN = os.getenv("BOT_TOKEN")
CHAT_ID = os.getenv("CHAT_ID_GRUPO")


INTERVALO_SEGUNDOS = 60
STATE_FILE = Path("last_state.json")

# ==========================
# SQL DA VIEW
# ==========================
SQL = """
select
    nr_atendimento,
    nm_paciente,
    total_pontos
from HC_SCORE_FARMACIA
"""

# ==========================
# FUNÇÕES
# ==========================
def enviar_telegram(mensagem: str):
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": CHAT_ID,
        "text": mensagem,
        "parse_mode": "HTML"
    }
    requests.post(url, json=payload, timeout=10)


def buscar_scores():
    conn = oracledb.connect(
        user=USUARIO,
        password=SENHA,
        dsn=DSN
    )

    cur = conn.cursor()
    cur.execute(SQL)

    dados = {}
    for nr_atendimento, nm_paciente, score_total in cur.fetchall():
        dados[str(nr_atendimento)] = {
            "paciente": nm_paciente,
            "score": score_total
        }

    cur.close()
    conn.close()
    return dados


def carregar_estado_anterior():
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def salvar_estado_atual(estado):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)


def comparar_estados(anterior, atual):
    for atendimento, dados_atual in atual.items():
        dados_ant = anterior.get(atendimento)

        # Novo paciente
        if not dados_ant:
            msg = (
                f"🆕 <b>Novo paciente monitorado</b>\n"
                f"Atendimento: {atendimento}\n"
                f"Paciente: {dados_atual['paciente']}\n"
                f"Score: {dados_atual['score']}"
            )
            print(msg)
            enviar_telegram(msg)
            continue

        # Alteração de score
        if dados_ant["score"] != dados_atual["score"]:
            msg = (
                f"⚠️ <b>ALTERAÇÃO DE SCORE</b>\n"
                f"🕒 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
                f"<b>Atendimento:</b> {atendimento}\n"
                f"<b>Paciente:</b> {dados_atual['paciente']}\n"
                f"<b>Score anterior:</b> {dados_ant['score']}\n"
                f"<b>Score atual:</b> {dados_atual['score']}"
            )
            print(msg)
            enviar_telegram(msg)


# ==========================
# LOOP PRINCIPAL
# ==========================
def main():
    print("📡 Monitor de Score da Farmácia com Telegram iniciado...\n")

    estado_anterior = carregar_estado_anterior()

    while True:
        try:
            estado_atual = buscar_scores()
            comparar_estados(estado_anterior, estado_atual)
            salvar_estado_atual(estado_atual)
            estado_anterior = estado_atual
            time.sleep(INTERVALO_SEGUNDOS)

        except Exception as e:
            print("❌ Erro no monitoramento:", e)
            time.sleep(10)


if __name__ == "__main__":
    main()
