# Extração de Relatório - Boletos por Condomínio
# Sprint 2 - Projeto Aplicado
# Gera: Detalhado + Resumo por Unidade

import pyodbc
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("EXTRAÇÃO DE RELATÓRIOS - SPRINT 2")
print("=" * 60)

# Ler lista de condomínios do Excel
arquivo_condominios = os.path.join(os.path.dirname(__file__), 'condominios.xlsx')
df_condominios = pd.read_excel(arquivo_condominios)
condominios_ativos = df_condominios[df_condominios['Ativo'] == True]['Condominio'].tolist()

print(f"Condomínios a processar: {condominios_ativos}")
print("=" * 60)

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

# Query de boletos detalhado
QUERY_DETALHADO = '''
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

def gerar_resumo_unidade(df_detalhado):
    """Gera resumo por unidade a partir do detalhado"""

    resumo = df_detalhado.groupby('Unidade').agg(
        Total_Boletos=('NossoNumCNAB', 'count'),
        Meses_Em_Aberto=('Status', lambda x: (x == 'Em Aberto').sum()),
        Meses_Pagos=('Status', lambda x: (x == 'Baixado').sum()),
        Pagos_Em_Dia=('TipoDif', lambda x: (x == 'Pago exato').sum()),
        Pagos_Com_Juros=('TipoDif', lambda x: (x == 'Pagou a mais (juros/multa)').sum()),
        Total_Juros_Pagos=('DifValor', lambda x: x[x > 0].sum() if (x > 0).any() else 0),
        Valor_Em_Aberto=('Valor', lambda x: df_detalhado.loc[x.index][df_detalhado.loc[x.index, 'Status'] == 'Em Aberto']['Valor'].sum()),
    ).reset_index()

    # Calcular valor em aberto corretamente
    valor_aberto = df_detalhado[df_detalhado['Status'] == 'Em Aberto'].groupby('Unidade')['Valor'].sum().reset_index()
    valor_aberto.columns = ['Unidade', 'Valor_Em_Aberto']

    # Remover coluna calculada errada e fazer merge
    resumo = resumo.drop('Valor_Em_Aberto', axis=1)
    resumo = resumo.merge(valor_aberto, on='Unidade', how='left')
    resumo['Valor_Em_Aberto'] = resumo['Valor_Em_Aberto'].fillna(0)

    # Status da unidade
    resumo['Status_Unidade'] = resumo['Meses_Em_Aberto'].apply(
        lambda x: 'Inadimplente' if x > 0 else 'Adimplente'
    )

    # Ordenar colunas
    resumo = resumo[[
        'Unidade', 'Total_Boletos', 'Meses_Em_Aberto', 'Meses_Pagos',
        'Pagos_Em_Dia', 'Pagos_Com_Juros', 'Total_Juros_Pagos',
        'Valor_Em_Aberto', 'Status_Unidade'
    ]]

    return resumo

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

        # Extrair detalhado
        query = QUERY_DETALHADO.format(condominio=condominio)
        df_detalhado = pd.read_sql(query, conn)
        print(f"  -> Detalhado: {len(df_detalhado)} registros")

        # Gerar resumo por unidade
        df_resumo = gerar_resumo_unidade(df_detalhado)
        print(f"  -> Resumo: {len(df_resumo)} unidades")

        # Estatísticas
        adimplentes = (df_resumo['Status_Unidade'] == 'Adimplente').sum()
        inadimplentes = (df_resumo['Status_Unidade'] == 'Inadimplente').sum()
        print(f"  -> Adimplentes: {adimplentes} | Inadimplentes: {inadimplentes}")

        # Salvar Excel com 2 abas
        nome_arquivo = condominio.replace(' ', '_').lower()
        arquivo = os.path.join(output_dir, f'relatorio_{nome_arquivo}_{timestamp}.xlsx')

        with pd.ExcelWriter(arquivo, engine='openpyxl') as writer:
            df_detalhado.to_excel(writer, sheet_name='Detalhado', index=False)
            df_resumo.to_excel(writer, sheet_name='Resumo por Unidade', index=False)

        print(f"  -> Salvo: {arquivo}")

    conn.close()
    print("\n" + "=" * 60)
    print("[OK] Extração concluída!")
    print("=" * 60)

except Exception as e:
    print(f"[ERRO] {e}")
    import traceback
    traceback.print_exc()
