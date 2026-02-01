# Extração de Relatório CONSOLIDADO - Todos os Condomínios
# Sprint 2 - Projeto Aplicado
# Gera visão geral de TODOS os condomínios

import pyodbc
import pandas as pd
import os
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

print("=" * 60)
print("RELATÓRIO CONSOLIDADO - TODOS OS CONDOMÍNIOS")
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

# Ler lista de condomínios do Excel
arquivo_condominios = os.path.join(os.path.dirname(__file__), 'condominios.xlsx')
df_condominios = pd.read_excel(arquivo_condominios)
condominios_ativos = df_condominios[df_condominios['Ativo'] == True]['Condominio'].tolist()
filtro_condominios = "','".join(condominios_ativos)

print(f"Condomínios a processar: {condominios_ativos}")

# Query consolidado por condomínio (filtrado)
QUERY_CONSOLIDADO = f'''
SELECT
    co.Resumo AS Condominio,
    COUNT(DISTINCT l.NumLoja) AS Total_Unidades,
    COUNT(DISTINCT CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN l.NumLoja END) AS Unidades_Inadimplentes,
    COUNT(CASE WHEN b.DataPagamento IS NOT NULL THEN 1 END) AS Boletos_Pagos,
    COUNT(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN 1 END) AS Boletos_Em_Aberto,
    ISNULL(SUM(CASE WHEN b.DataPagamento IS NOT NULL THEN b.ValorPagamento END), 0) AS Valor_Total_Pago,
    ISNULL(SUM(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN b.Valor END), 0) AS Valor_Total_Em_Aberto,
    ISNULL(SUM(CASE
        WHEN b.DataPagamento IS NOT NULL AND b.ValorPagamento > b.Valor
        THEN b.ValorPagamento - b.Valor
        ELSE 0
    END), 0) AS Total_Juros_Recebidos
FROM Boleto b
INNER JOIN Contas c ON c.NossoNumCNAB = b.NossoNumCNAB
INNER JOIN Condominios co ON co.IDCond = c.IDCond
INNER JOIN Lojas l ON l.IDCond = c.IDCond AND l.NumLoja = c.NumLoja
WHERE b.Situacao NOT IN (20, 30)
  AND co.Resumo IN ('{filtro_condominios}')
GROUP BY co.Resumo
ORDER BY Valor_Total_Em_Aberto DESC
'''

# Query detalhado de inadimplência por condomínio (filtrado)
QUERY_INADIMPLENCIA = f'''
SELECT
    co.Resumo AS Condominio,
    l.NumLoja AS Unidade,
    COUNT(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN 1 END) AS Meses_Em_Aberto,
    ISNULL(SUM(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN b.Valor END), 0) AS Valor_Em_Aberto,
    MIN(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN b.MesReferencia END) AS Mes_Mais_Antigo
FROM Boleto b
INNER JOIN Contas c ON c.NossoNumCNAB = b.NossoNumCNAB
INNER JOIN Condominios co ON co.IDCond = c.IDCond
INNER JOIN Lojas l ON l.IDCond = c.IDCond AND l.NumLoja = c.NumLoja
WHERE b.Situacao = 10 AND b.DataPagamento IS NULL
  AND co.Resumo IN ('{filtro_condominios}')
GROUP BY co.Resumo, l.NumLoja
HAVING COUNT(CASE WHEN b.Situacao = 10 AND b.DataPagamento IS NULL THEN 1 END) > 0
ORDER BY co.Resumo, Valor_Em_Aberto DESC
'''

try:
    print("Conectando ao SQL Server...")
    conn = pyodbc.connect(conn_str)
    print("[OK] Conectado!")

    # Extrair consolidado
    print("\n[1/2] Extraindo consolidado por condomínio...")
    df_consolidado = pd.read_sql(QUERY_CONSOLIDADO, conn)
    print(f"  -> {len(df_consolidado)} condomínios")

    # Calcular totais
    total_unidades = df_consolidado['Total_Unidades'].sum()
    total_inadimplentes = df_consolidado['Unidades_Inadimplentes'].sum()
    total_em_aberto = df_consolidado['Valor_Total_Em_Aberto'].sum()

    print(f"  -> Total unidades: {total_unidades}")
    print(f"  -> Unidades inadimplentes: {total_inadimplentes}")
    print(f"  -> Valor em aberto: R$ {total_em_aberto:,.2f}")

    # Adicionar % inadimplência
    df_consolidado['Perc_Inadimplencia'] = (
        df_consolidado['Unidades_Inadimplentes'] / df_consolidado['Total_Unidades'] * 100
    ).round(2)

    # Extrair detalhado de inadimplência
    print("\n[2/2] Extraindo detalhado de inadimplência...")
    df_inadimplencia = pd.read_sql(QUERY_INADIMPLENCIA, conn)
    print(f"  -> {len(df_inadimplencia)} unidades inadimplentes")

    # Diretório de saída
    output_dir = os.path.join(os.path.dirname(__file__), 'output')
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

    # Criar resumo geral
    df_resumo = pd.DataFrame({
        'Métrica': [
            'Total de Condomínios',
            'Total de Unidades',
            'Unidades Inadimplentes',
            'Taxa de Inadimplência',
            'Valor Total em Aberto',
            'Valor Total Pago',
            'Total Juros Recebidos'
        ],
        'Valor': [
            len(df_consolidado),
            total_unidades,
            total_inadimplentes,
            f"{(total_inadimplentes/total_unidades*100):.2f}%",
            f"R$ {total_em_aberto:,.2f}",
            f"R$ {df_consolidado['Valor_Total_Pago'].sum():,.2f}",
            f"R$ {df_consolidado['Total_Juros_Recebidos'].sum():,.2f}"
        ]
    })

    # Salvar Excel com 3 abas
    arquivo = os.path.join(output_dir, f'consolidado_geral_{timestamp}.xlsx')

    with pd.ExcelWriter(arquivo, engine='openpyxl') as writer:
        df_resumo.to_excel(writer, sheet_name='Resumo Geral', index=False)
        df_consolidado.to_excel(writer, sheet_name='Por Condomínio', index=False)
        df_inadimplencia.to_excel(writer, sheet_name='Inadimplentes', index=False)

    print(f"\n[OK] Arquivo salvo: {arquivo}")

    conn.close()
    print("\n" + "=" * 60)
    print("[OK] Consolidado concluído!")
    print("=" * 60)

except Exception as e:
    print(f"[ERRO] {e}")
    import traceback
    traceback.print_exc()
