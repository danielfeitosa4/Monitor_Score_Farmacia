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
# SQL DA VIEW DETALHADA
# ==========================
SQL = """
SELECT
    nr_atendimento,
    nm_paciente,
    idade,
    creatinina,
    total_pontos,
    qtd_medicamentos,
    nm_medicamentos,
    qtd_medicamentos_mav,
    nm_medicamentos_mav,
    qtd_dispositivo,
    nm_dispositivo,
    qtd_parenteral,
    nm_parenteral
FROM hc_score_farmacia_det
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


def normalizar_lista(valor):
    if not valor:
        return []
    return [v.strip() for v in valor.split(",") if v.strip()]


def buscar_scores():
    conn = oracledb.connect(
        user=USUARIO,
        password=SENHA,
        dsn=DSN
    )

    cur = conn.cursor()
    cur.execute(SQL)

    dados = {}

    def safe(valor, padrao=""):
        return valor if valor is not None else padrao


    for row in cur.fetchall():
        (
            nr_atendimento,
            nm_paciente,
            idade,
            creatinina,
            total_pontos,
            qtd_medicamentos,
            nm_medicamentos,
            qtd_mav,
            nm_mav,
            qtd_dispositivo,
            nm_dispositivo,
            qtd_parenteral,
            nm_parenteral
        ) = row

        dados[str(nr_atendimento)] = {
            "paciente": safe(nm_paciente),
            "idade": safe(idade),
            "creatinina": safe(creatinina),
            "score": safe(total_pontos),
            "qtd_medicamentos": safe(qtd_medicamentos),
            "nm_medicamentos": safe(nm_medicamentos),
            "qtd_mav": safe(qtd_mav),
            "nm_mav": safe(nm_mav),
            "qtd_dispositivo": safe(qtd_dispositivo),
            "nm_dispositivo": safe(nm_dispositivo),
            "qtd_parenteral": safe(qtd_parenteral),
            "nm_parenteral": safe(nm_parenteral)
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

        if dados_ant != dados_atual:
            enviar_snapshot_completo(atendimento, dados_atual)
            
def enviar_snapshot_completo(atendimento, d):
    msg = (
        f"⚠️ <b>ATUALIZAÇÃO CLÍNICA DO PACIENTE</b>\n\n"
        f"🕒 {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n\n"
        f"📌 <b>Atendimento:</b> {atendimento}\n"
        f"👤 <b>Paciente:</b> {d['paciente']}\n"
        f"🎂 <b>Idade:</b> {d['idade']} anos\n"
        f"🧪 <b>Creatinina:</b> {d['creatinina']}\n\n"
        f"🔢 <b>SCORE TOTAL ATUAL:</b> <b>{d['score']}</b>\n\n"
        f"💊 <b>Medicamentos:</b> {d['qtd_medicamentos']}\n"
        f"{d['nm_medicamentos']}\n\n"
        f"🚨 <b>Alta Vigilância (MAV):</b> {d['qtd_mav']}\n"
        f"{d['nm_mav']}\n\n"
        f"🧰 <b>Dispositivos:</b> {d['qtd_dispositivo']}\n"
        f"{d['nm_dispositivo']}\n\n"
        f"🥣 <b>Nutrição Parenteral:</b> {d['qtd_parenteral']}\n"
        f"{d['nm_parenteral']}"
    )

    print(msg)
    enviar_telegram(msg)



# ==========================
# LOOP PRINCIPAL
# ==========================
def main():
    print("📡 Monitor de Score da Farmácia (DETALHADO) iniciado...\n")

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
