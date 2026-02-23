"""
DAG: Relatórios Semanais - Projeto Aplicado
Executa toda segunda-feira às 06:00
Pipeline ETL completo com conexão SQL Server
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import os
import sys

# Adicionar pasta dos scripts ao path
sys.path.insert(0, '/opt/airflow/scripts')

# Configurações padrão
default_args = {
    'owner': 'fernando',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def executar_relatorio_individual():
    """Executa o script de relatório individual por condomínio"""
    import subprocess

    script_path = '/opt/airflow/scripts/extrair_relatorio.py'
    print(f"Executando: {script_path}")

    result = subprocess.run(
        ['python', script_path],
        cwd='/opt/airflow/scripts',
        capture_output=True,
        text=True,
        env={**os.environ}
    )

    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Script falhou com código {result.returncode}")

    return "relatorio_individual_gerado"

def executar_relatorio_consolidado():
    """Executa o script de relatório consolidado"""
    import subprocess

    script_path = '/opt/airflow/scripts/extrair_consolidado.py'
    print(f"Executando: {script_path}")

    result = subprocess.run(
        ['python', script_path],
        cwd='/opt/airflow/scripts',
        capture_output=True,
        text=True,
        env={**os.environ}
    )

    print("STDOUT:", result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)

    if result.returncode != 0:
        raise Exception(f"Script falhou com código {result.returncode}")

    return "relatorio_consolidado_gerado"

def validar_relatorios():
    """Valida se os relatórios foram gerados"""
    import glob
    from datetime import datetime

    output_dir = '/opt/airflow/scripts/output'
    hoje = datetime.now().strftime('%Y%m%d')

    # Verificar arquivos gerados hoje
    arquivos = glob.glob(f"{output_dir}/*{hoje}*.xlsx")

    if arquivos:
        print(f"Relatórios gerados: {len(arquivos)} arquivo(s)")
        for arq in arquivos:
            print(f"  - {os.path.basename(arq)}")
        return "validacao_ok"
    else:
        print("Nenhum relatório encontrado para hoje")
        return "sem_relatorios"

def notificar_conclusao(**context):
    """Notifica conclusão do pipeline"""
    print("=" * 60)
    print("PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 60)
    print(f"Data de execução: {datetime.now()}")
    print("Relatórios disponíveis em: /opt/airflow/scripts/output/")
    return "notificacao_enviada"

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

    relatorio_individual = PythonOperator(
        task_id='relatorio_individual',
        python_callable=executar_relatorio_individual,
    )

    relatorio_consolidado = PythonOperator(
        task_id='relatorio_consolidado',
        python_callable=executar_relatorio_consolidado,
    )

    validar = PythonOperator(
        task_id='validar_relatorios',
        python_callable=validar_relatorios,
    )

    notificar = PythonOperator(
        task_id='notificar_conclusao',
        python_callable=notificar_conclusao,
    )

    fim = EmptyOperator(task_id='fim')

    # Ordem de execução
    inicio >> relatorio_individual >> relatorio_consolidado >> validar >> notificar >> fim
