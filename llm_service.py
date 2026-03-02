from openai import OpenAI

client = OpenAI(
    base_url="http://localhost:1234/v1",
    api_key="lm-studio"
)

MODEL = "meta-llama-3.1-8b-instruct"

def gerar_sql(pergunta):

    prompt = """
Você é um assistente hospitalar.
Use a view hc_score_farmacia com colunas:
nr_atendimento, nm_paciente, score_total, idade, creatinina, atb_ev, unidade.

Retorne apenas SQL Oracle válido.
Somente SELECT.
"""

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": prompt},
            {"role": "user", "content": pergunta}
        ],
        temperature=0
    )

    return response.choices[0].message.content.strip()


def formatar_resposta(dados):

    response = client.chat.completions.create(
        model=MODEL,
        messages=[
            {"role": "system", "content": "Explique os dados para a equipe da farmácia clínica."},
            {"role": "user", "content": f"Dados retornados: {dados}"}
        ],
        temperature=0.3
    )

    return response.choices[0].message.content