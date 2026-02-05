import oracledb
from config.settings import ORACLE_USER, ORACLE_PASSWORD, ORACLE_DSN


def get_connection():
  return oracledb.connect(
user=ORACLE_USER,
password=ORACLE_PASSWORD,
dsn=ORACLE_DSN
)