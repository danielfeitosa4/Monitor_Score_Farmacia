# Monitor de Score da Farmácia

Projeto em Python para monitorar alterações no score clínico de pacientes
a partir de uma view Oracle (TASY) e enviar alertas automáticos via Telegram.

## Tecnologias
- Python 3.11+
- Oracle Database
- python-oracledb (thick mode)
- Telegram Bot API

## Configuração
```bash
setx ORACLE_USER "usuario"
setx ORACLE_PASS "senha"
setx TELEGRAM_BOT_TOKEN "token"
setx TELEGRAM_CHAT_ID "-123456"
