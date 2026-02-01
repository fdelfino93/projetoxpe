"""
DAG: Relatórios Semanais - Projeto Aplicado
Executa toda segunda-feira às 06:00
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# Configurações padrão
default_args = {
    'owner': 'fernando',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Funções das tarefas
def extrair_dados():
    """Extrai dados do SQL Server"""
    print("Extraindo dados do SQL Server...")
    print("Conectando em 192.168.1.226/Cond21")
    print("Extração concluída!")
    return "dados_extraidos"

def transformar_dados(**context):
    """Transforma e consolida os dados"""
    print("Transformando dados...")
    print("Gerando resumo por unidade...")
    print("Gerando consolidado por condomínio...")
    print("Transformação concluída!")
    return "dados_transformados"

def gerar_relatorios(**context):
    """Gera os relatórios em Excel"""
    print("Gerando relatórios Excel...")
    print("- Relatório individual por unidade")
    print("- Relatório consolidado")
    print("Relatórios gerados!")
    return "relatorios_gerados"

def enviar_email(**context):
    """Envia relatórios por email"""
    print("Enviando relatórios por email...")
    print("Email enviado para diretoria!")
    return "email_enviado"

# Definição da DAG
with DAG(
    'relatorios_semanais',
    default_args=default_args,
    description='Pipeline de relatórios semanais - Projeto Aplicado XPE',
    schedule_interval='0 6 * * 1',  # Segunda-feira às 06:00
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['projeto-aplicado', 'relatorios', 'etl'],
) as dag:

    # Tarefas
    inicio = EmptyOperator(task_id='inicio')

    extrair = PythonOperator(
        task_id='extrair_dados',
        python_callable=extrair_dados,
    )

    transformar = PythonOperator(
        task_id='transformar_dados',
        python_callable=transformar_dados,
    )

    gerar = PythonOperator(
        task_id='gerar_relatorios',
        python_callable=gerar_relatorios,
    )

    enviar = PythonOperator(
        task_id='enviar_email',
        python_callable=enviar_email,
    )

    fim = EmptyOperator(task_id='fim')

    # Ordem de execução
    inicio >> extrair >> transformar >> gerar >> enviar >> fim
