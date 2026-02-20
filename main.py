import oracledb
import json
import time
import requests
from pathlib import Path
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

COOLDOWN_MINUTOS = 30
ultimo_alerta = {}

def pode_notificar(nr_atendimento):
    agora = datetime.now()

    if nr_atendimento not in ultimo_alerta:
        return True

    ultima_vez = ultimo_alerta[nr_atendimento]
    return agora - ultima_vez >= timedelta(minutes=COOLDOWN_MINUTOS)

def comparar_estados(anterior, atual):
    for atendimento, dados_atual in atual.items():
        dados_ant = anterior.get(atendimento)

        if not dados_ant:
            continue

        if not pode_notificar(atendimento):
            continue

        blocos = []

        # ==========================
        # VALORES SIMPLES
        # ==========================
        blocos.append(
            diff_valor("Idade", dados_ant["idade"], dados_atual["idade"], " anos")
        )

        blocos.append(
            diff_valor("Creatinina", dados_ant["creatinina"], dados_atual["creatinina"])
        )

        blocos.append(
            diff_valor("Score Total", dados_ant["score"], dados_atual["score"])
        )

        # ==========================
        # LISTAS
        # ==========================
        blocos.append(
            diff_lista(
                "Medicamentos",
                dados_ant["qtd_medicamentos"],
                dados_ant["nm_medicamentos"],
                dados_atual["qtd_medicamentos"],
                dados_atual["nm_medicamentos"]
            )
        )

        blocos.append(
            diff_lista(
                "Medicamentos EV",
                dados_ant["qtd_medicamentos_ev"],
                dados_ant["nm_medicamentos_ev"],
                dados_atual["qtd_medicamentos_ev"],
                dados_atual["nm_medicamentos_ev"]
            )
        )

        blocos.append(
            diff_lista(
                "Antibióticos (ATB)",
                dados_ant["qtd_medicamentos_atb"],
                dados_ant["nm_medicamentos_atb"],
                dados_atual["qtd_medicamentos_atb"],
                dados_atual["nm_medicamentos_atb"]
            )
        )

        blocos.append(
            diff_lista(
                "Alta Vigilância (MAV)",
                dados_ant["qtd_medicamentos_mav"],
                dados_ant["nm_medicamentos_mav"],
                dados_atual["qtd_medicamentos_mav"],
                dados_atual["nm_medicamentos_mav"]
            )
        )

        blocos.append(
            diff_lista(
                "Dispositivos",
                dados_ant["qtd_dispositivo"],
                dados_ant["nm_dispositivo"],
                dados_atual["qtd_dispositivo"],
                dados_atual["nm_dispositivo"]
            )
        )

        blocos.append(
            diff_lista(
                "Nutrição Parenteral",
                dados_ant["qtd_parenteral"],
                dados_ant["nm_parenteral"],
                dados_atual["qtd_parenteral"],
                dados_atual["nm_parenteral"]
            )
        )

        # ==========================
        # FINAL
        # ==========================
        blocos = [b for b in blocos if b]

        if not blocos:
            continue

        mensagem_diferencas = "".join(blocos)

        enviar_snapshot_completo(
            atendimento,
            dados_atual,
            mensagem_diferencas
        )

        ultimo_alerta[atendimento] = datetime.now()


            
def diff_lista(titulo, qtd_ant, lista_ant, qtd_atual, lista_atual, emoji="💊"):
    ant_set = set(normalizar_lista(lista_ant))
    atual_set = set(normalizar_lista(lista_atual))

    if ant_set == atual_set:
        return ""

    adicionados = atual_set - ant_set
    removidos = ant_set - atual_set

    bloco = (
        f"{emoji} <b>{titulo}:</b>\n"
        f"• Antes: {qtd_ant}\n"
        f"• Agora: {qtd_atual}\n"
    )

    if adicionados:
        bloco += "\n➕ <b>Adicionados:</b>\n"
        bloco += "\n".join(f"  • {i}" for i in adicionados)

    if removidos:
        bloco += "\n\n➖ <b>Removidos:</b>\n"
        bloco += "\n".join(f"  • {i}" for i in removidos)

    return bloco



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
    cd_unidade_basica,
    idade,
    ponto_idade,
    creatinina,
    ponto_creatinina,
    total_pontos,
    qtd_medicamentos,
    nm_medicamentos,
    ponto_medicamentos,
    qtd_medicamentos_mav,
    nm_medicamentos_mav,
    ponto_medicamentos_mav,
    qtd_medicamentos_ev,
    nm_medicamentos_ev,
    ponto_medicamentos_ev,
    qtd_medicamentos_atb,
    nm_medicamentos_atb,
    ponto_medicamentos_atb,
    qtd_dispositivo,
    nm_dispositivo,
    ponto_dispositivo,
    qtd_parenteral,
    nm_parenteral,
    ponto_parenteral
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
            cd_unidade_basica,
            idade,
            ponto_idade,
            creatinina,
            ponto_creatinina,
            total_pontos,
            qtd_medicamentos,
            nm_medicamentos,
            ponto_medicamentos,
            qtd_medicamentos_mav,
            nm_medicamentos_mav,
            ponto_medicamentos_mav,
            qtd_medicamentos_ev,
            nm_medicamentos_ev,
            ponto_medicamentos_ev,
            qtd_medicamentos_atb,
            nm_medicamentos_atb,
            ponto_medicamentos_atb,
            qtd_dispositivo,
            nm_dispositivo,
            ponto_dispositivo,
            qtd_parenteral,
            nm_parenteral,
            ponto_parenteral
        ) = row

        dados[str(nr_atendimento)] = {
            "paciente": safe(nm_paciente),
            "unidade": safe(cd_unidade_basica),
            "idade": safe(idade),
            "ponto_idade": safe(ponto_idade),
            "creatinina": safe(creatinina),
            "ponto_creatinina": safe(ponto_creatinina),
            "score": safe(total_pontos),
            "qtd_medicamentos": safe(qtd_medicamentos),
            "nm_medicamentos": safe(nm_medicamentos),
            "ponto_medicamentos": safe(ponto_medicamentos),
            "qtd_medicamentos_mav": safe(qtd_medicamentos_mav),
            "nm_medicamentos_mav": safe(nm_medicamentos_mav),
            "ponto_medicamentos_mav": safe(ponto_medicamentos_mav),
            "qtd_medicamentos_ev": safe(qtd_medicamentos_ev),
            "nm_medicamentos_ev": safe(nm_medicamentos_ev),
            "ponto_medicamentos_ev": safe(ponto_medicamentos_ev),
            "qtd_medicamentos_atb": safe(qtd_medicamentos_atb),
            "nm_medicamentos_atb": safe(nm_medicamentos_atb),
            "ponto_medicamentos_atb": safe(ponto_medicamentos_atb),
            "qtd_dispositivo": safe(qtd_dispositivo),
            "nm_dispositivo": safe(nm_dispositivo),
            "ponto_dispositivo": safe(ponto_dispositivo),
            "qtd_parenteral": safe(qtd_parenteral),
            "nm_parenteral": safe(nm_parenteral),
            "ponto_parenteral": safe(ponto_parenteral)
        }

    cur.close()
    conn.close()
    return dados

def formatar_lista_em_linhas(valor):
    if not valor:
        return "—"

    itens = [v.strip() for v in valor.split(",") if v.strip()]
    return "\n".join(f"• {item}" for item in itens)


def carregar_estado_anterior():
    if not STATE_FILE.exists():
        return {}
    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def salvar_estado_atual(estado):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(estado, f, ensure_ascii=False, indent=2)
        
def diff_valor(titulo, valor_ant, valor_atual, sufixo=""):
    if valor_ant == valor_atual:
        return ""

    return (
        f"🔄 <b>{titulo}:</b>\n"
        f"• Antes: {valor_ant}{sufixo}\n"
        f"• Agora: {valor_atual}{sufixo}\n\n"
    )

def gerar_diferencas(dados_ant, dados_atual):
    if not dados_ant:
        return "📌 Primeira leitura registrada para este paciente."

    diferencas = []

    # ==========================
    # IDADE
    # ==========================
    bloco = diff_valor(
        "Idade",
        dados_ant.get("idade"),
        dados_atual.get("idade"),
        " anos"
    )
    if bloco:
        diferencas.append(bloco)

    # ==========================
    # CREATININA
    # ==========================
    bloco = diff_valor(
        "Creatinina",
        dados_ant.get("creatinina"),
        dados_atual.get("creatinina"),
    )
    if bloco:
        diferencas.append(bloco)

    # ==========================
    # MEDICAMENTOS
    # ==========================
    bloco = diff_lista(
        "Medicamentos Gerais",
        dados_ant.get("qtd_medicamentos"),
        dados_ant.get("nm_medicamentos"),
        dados_atual.get("qtd_medicamentos"),
        dados_atual.get("nm_medicamentos"),
        emoji="💊"
    )
    if bloco:
        diferencas.append(bloco)
        
    # ==========================
    # MEDICAMENTOS EV
    # ==========================
    bloco = diff_lista(
        "Medicamentos EV",
        dados_ant.get("qtd_medicamentos_ev"),
        dados_ant.get("nm_medicamentos_ev"),
        dados_atual.get("qtd_medicamentos_ev"),
        dados_atual.get("nm_medicamentos_ev"),
        emoji="💉"
    )
    if bloco:
        diferencas.append(bloco)
        
    # ==========================
    # MEDICAMENTOS ATB
    # ==========================
    bloco = diff_lista(
        "Medicamentos ATB",
        dados_ant.get("qtd_medicamentos_atb"),
        dados_ant.get("nm_medicamentos_atb"),
        dados_atual.get("qtd_medicamentos_atb"),
        dados_atual.get("nm_medicamentos_atb"),
        emoji="🧫"
    )
    if bloco:
        diferencas.append(bloco)
        
    # ==========================
    # MEDICAMENTOS MAV
    # ==========================
    bloco = diff_lista(
        "Medicamentos MAV",
        dados_ant.get("qtd_medicamentos_mav"),
        dados_ant.get("nm_medicamentos_mav"),
        dados_atual.get("qtd_medicamentos_mav"),
        dados_atual.get("nm_medicamentos_mav"),
        emoji="🚨"
    )
    if bloco:
        diferencas.append(bloco)

    # ==========================
    # DISPOSITIVOS
    # ==========================
    bloco = diff_lista(
        "Dispositivos",
        dados_ant.get("qtd_dispositivo"),
        dados_ant.get("nm_dispositivo"),
        dados_atual.get("qtd_dispositivo"),
        dados_atual.get("nm_dispositivo"),
        emoji="🧰"
    )
    if bloco:
        diferencas.append(bloco)

    # ==========================
    # NUTRIÇÃO PARENTERAL
    # ==========================
    bloco = diff_lista(
        "Nutrição Parenteral",
        dados_ant.get("qtd_parenteral"),
        dados_ant.get("nm_parenteral"),
        dados_atual.get("qtd_parenteral"),
        dados_atual.get("nm_parenteral"),
        emoji="🥣"
    )
    if bloco:
        diferencas.append(bloco)

    return "\n\n".join(diferencas)



def comparar_estados(anterior, atual):
    for atendimento, dados_atual in atual.items():
        dados_ant = anterior.get(atendimento)

        if dados_ant != dados_atual and pode_notificar(atendimento):
            diferencas = gerar_diferencas(dados_ant, dados_atual)
            enviar_snapshot_completo(atendimento, dados_atual, diferencas)
            ultimo_alerta[atendimento] = datetime.now()

            
def enviar_snapshot_completo(atendimento, d, diferencas=""):
    msg = (
        f"⚠️ <b>ATUALIZAÇÃO CLÍNICA DO PACIENTE</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"🕒 <i>{datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</i>\n\n"

        f"📌 <b>Atendimento:</b> {atendimento}\n"
        f"🏥 <b>Unidade:</b> {d['unidade']}\n"
        f"👤 <b>Paciente:</b> {d['paciente']}\n"
        f"🎂 <b>Idade:</b> {d['idade']} anos\n"
        f"🧪 <b>Creatinina:</b> {d['creatinina']}\n\n"

        f"🔢 <b>SCORE TOTAL ATUAL:</b> <b>{d['score']}</b>\n"
        f"━━━━━━━━━━━━━━━━━━━━━━\n\n"

        f"💊 <b>Medicamentos ({d['qtd_medicamentos']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_medicamentos'])}\n\n"
        
        f"🧫 <b>Antibióticos ({d['qtd_medicamentos_atb']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_medicamentos_atb'])}\n\n"
        
        f"💉 <b>Endovenosos ({d['qtd_medicamentos_ev']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_medicamentos_ev'])}\n\n"

        f"🚨 <b>Alta Vigilância – MAV ({d['qtd_medicamentos_mav']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_medicamentos_mav'])}\n\n"

        f"🧰 <b>Dispositivos ({d['qtd_dispositivo']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_dispositivo'])}\n\n"

        f"🥣 <b>Nutrição Parenteral ({d['qtd_parenteral']}):</b>\n"
        f"{formatar_lista_em_linhas(d['nm_parenteral'])}\n\n"
    )

    if diferencas:
        msg += (
            "\n\n━━━━━━━━━━━━━━━━━━━━━━\n\n"
            "🆕 <b>O QUE MUDOU NESSA ATUALIZAÇÃO:</b>\n\n"
            f"{diferencas}"
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
