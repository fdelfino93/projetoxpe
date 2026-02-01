# Extração de Relatório - Boletos por Condomínio
# Sprint 1 - Projeto Aplicado
# Lê lista de condomínios de condominios.xlsx

import pyodbc
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

print("=" * 50)
print("EXTRAÇÃO DE RELATÓRIOS")
print("=" * 50)

# Ler lista de condomínios do Excel
arquivo_condominios = os.path.join(os.path.dirname(__file__), 'condominios.xlsx')
df_condominios = pd.read_excel(arquivo_condominios)
condominios_ativos = df_condominios[df_condominios['Ativo'] == True]['Condominio'].tolist()

print(f"Condomínios a processar: {condominios_ativos}")
print("=" * 50)

# Conexão
server = os.getenv('DB_SERVER')
database = os.getenv('DB_DATABASE')
username = os.getenv('DB_USER')
password = os.getenv('DB_PASSWORD')
driver = 'ODBC Driver 17 for SQL Server'

conn_str = (
    f"DRIVER={{{driver}}};"
    f"SERVER={server};"
    f"DATABASE={database};"
    f"UID={username};"
    f"PWD={password};"
)

# Query de boletos (com placeholder para condomínio)
QUERY = '''
SELECT
    co.Resumo AS Condominio,
    l.NumLoja AS Unidade,
    b.MesReferencia,
    CONVERT(VARCHAR(10), b.DataVencimento, 103) AS DataVencimento,
    CONVERT(VARCHAR(10), b.DataPagamento, 103) AS DataPagamento,
    b.NossoNumCNAB,
    b.Valor,
    b.ValorPagamento,
    CASE
        WHEN b.DataPagamento IS NOT NULL AND b.ValorPagamento IS NOT NULL
            THEN CAST(b.ValorPagamento - b.Valor AS DECIMAL(18,2))
        ELSE NULL
    END AS DifValor,
    CASE
        WHEN b.DataPagamento IS NULL OR b.ValorPagamento IS NULL
            THEN 'Saldo em Aberto'
        WHEN b.ValorPagamento - b.Valor > 0
            THEN 'Pagou a mais (juros/multa)'
        WHEN b.ValorPagamento - b.Valor < 0
            THEN 'Pagou a menos (desconto/parcial)'
        ELSE 'Pago exato'
    END AS TipoDif,
    CASE
        WHEN b.DataPagamento IS NOT NULL AND b.ValorPagamento IS NOT NULL
            THEN 'Baixado'
        WHEN b.Situacao = 10
            THEN 'Em Aberto'
        ELSE 'Outro'
    END AS Status
FROM Boleto b
INNER JOIN Contas c ON c.NossoNumCNAB = b.NossoNumCNAB
INNER JOIN Condominios co ON co.IDCond = c.IDCond
INNER JOIN Lojas l ON l.IDCond = c.IDCond AND l.NumLoja = c.NumLoja
WHERE
    co.Resumo LIKE '%{condominio}%'
    AND b.Situacao NOT IN (20, 30)
    AND (
        (b.DataPagamento IS NOT NULL AND b.ValorPagamento IS NOT NULL)
        OR (b.Situacao = 10 AND b.DataPagamento IS NULL)
    )
GROUP BY
    co.Resumo, l.NumLoja, b.MesReferencia, b.DataVencimento,
    b.DataPagamento, b.NossoNumCNAB, b.Valor, b.ValorPagamento, b.Situacao
ORDER BY l.NumLoja, b.DataVencimento
'''

try:
    print("Conectando ao SQL Server...")
    conn = pyodbc.connect(conn_str)
    print("[OK] Conectado!")

    # Diretório de saída
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Processar cada condomínio
    for condominio in condominios_ativos:
        print(f"\n[EXTRAINDO] {condominio}...")

        query = QUERY.format(condominio=condominio)
        df = pd.read_sql(query, conn)
        print(f"  -> {len(df)} registros")

        # Salvar Excel individual
        nome_arquivo = condominio.replace(' ', '_').lower()
        arquivo = os.path.join(output_dir, f'boletos_{nome_arquivo}_{timestamp}.xlsx')
        df.to_excel(arquivo, index=False, sheet_name=condominio[:30])
        print(f"  -> Salvo: {arquivo}")

    conn.close()
    print("\n" + "=" * 50)
    print("[OK] Extração concluída!")
    print("=" * 50)

except Exception as e:
    print(f"[ERRO] {e}")
