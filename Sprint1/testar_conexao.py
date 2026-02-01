# Teste de Conexão com SQL Server
# Sprint 1 - Projeto Aplicado

import pyodbc
import os
from dotenv import load_dotenv

load_dotenv()

print("=" * 50)
print("TESTE DE CONEXÃO - SQL SERVER")
print("=" * 50)

server = os.getenv('DB_SERVER', 'localhost')
database = os.getenv('DB_DATABASE', 'master')
username = os.getenv('DB_USER', '')
password = os.getenv('DB_PASSWORD', '')
driver = 'ODBC Driver 17 for SQL Server'

print(f"Servidor: {server}")
print(f"Database: {database}")
print(f"Usuário: {username}")
print("=" * 50)

try:
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
    )

    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()

    # Testar com query simples
    cursor.execute("SELECT @@VERSION")
    version = cursor.fetchone()[0]

    print("[OK] Conexão estabelecida com sucesso!")
    print(f"SQL Server: {version[:60]}...")

    cursor.close()
    conn.close()
    print("[OK] Conexão fechada.")

except Exception as e:
    print(f"[ERRO] Falha na conexão: {e}")
