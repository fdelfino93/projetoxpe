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

# ============================================================
# Configuração de email (R8)
# Status: STAND-BY - Aguardando cadastro de credenciais SMTP
# Para ativar: preencher as variáveis abaixo e alterar
#              ENVIO_EMAIL_ATIVO para True
# ============================================================
ENVIO_EMAIL_ATIVO = False

SMTP_CONFIG = {
    'servidor': os.environ.get('SMTP_SERVER', ''),
    'porta': int(os.environ.get('SMTP_PORT', '587')),
    'usuario': os.environ.get('SMTP_USER', ''),
    'senha': os.environ.get('SMTP_PASSWORD', ''),
    'remetente': os.environ.get('SMTP_FROM', ''),
    'destinatarios': os.environ.get('SMTP_TO', '').split(','),
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

    arquivos = glob.glob(f"{output_dir}/*{hoje}*.xlsx")

    if arquivos:
        print(f"Relatórios gerados: {len(arquivos)} arquivo(s)")
        for arq in arquivos:
            print(f"  - {os.path.basename(arq)}")
        return "validacao_ok"
    else:
        print("Nenhum relatório encontrado para hoje")
        return "sem_relatorios"


def enviar_email():
    """
    Envia relatórios por email (R8)
    Status: STAND-BY - Aguardando credenciais SMTP
    Para ativar: configurar ENVIO_EMAIL_ATIVO = True
    e preencher variáveis de ambiente SMTP_*
    """
    import glob
    from datetime import datetime

    if not ENVIO_EMAIL_ATIVO:
        print("=" * 60)
        print("ENVIO DE EMAIL: STAND-BY")
        print("Aguardando configuração de credenciais SMTP")
        print("Para ativar: ENVIO_EMAIL_ATIVO = True")
        print("=" * 60)
        return "email_standby"

    # Código pronto para quando ativar
    import smtplib
    from email.mime.multipart import MIMEMultipart
    from email.mime.base import MIMEBase
    from email.mime.text import MIMEText
    from email import encoders

    output_dir = '/opt/airflow/scripts/output'
    hoje = datetime.now().strftime('%Y%m%d')
    arquivos = glob.glob(f"{output_dir}/*{hoje}*.xlsx")

    msg = MIMEMultipart()
    msg['From'] = SMTP_CONFIG['remetente']
    msg['To'] = ', '.join(SMTP_CONFIG['destinatarios'])
    msg['Subject'] = f"Relatórios Semanais - {datetime.now().strftime('%d/%m/%Y')}"

    corpo = (
        f"Relatórios semanais gerados automaticamente em "
        f"{datetime.now().strftime('%d/%m/%Y às %H:%M')}.\n\n"
        f"Total de arquivos: {len(arquivos)}\n\n"
        f"Este email foi enviado automaticamente pelo pipeline ETL."
    )
    msg.attach(MIMEText(corpo, 'plain'))

    for arquivo in arquivos:
        with open(arquivo, 'rb') as f:
            part = MIMEBase('application', 'octet-stream')
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header(
                'Content-Disposition',
                f'attachment; filename={os.path.basename(arquivo)}'
            )
            msg.attach(part)

    with smtplib.SMTP(SMTP_CONFIG['servidor'], SMTP_CONFIG['porta']) as server:
        server.starttls()
        server.login(SMTP_CONFIG['usuario'], SMTP_CONFIG['senha'])
        server.send_message(msg)

    print(f"Email enviado para: {SMTP_CONFIG['destinatarios']}")
    print(f"Anexos: {len(arquivos)} arquivo(s)")
    return "email_enviado"


def notificar_conclusao(**context):
    """Notifica conclusão do pipeline"""
    print("=" * 60)
    print("PIPELINE CONCLUÍDO COM SUCESSO!")
    print("=" * 60)
    print(f"Data de execução: {datetime.now()}")
    print("Relatórios disponíveis em: /opt/airflow/scripts/output/")
    if not ENVIO_EMAIL_ATIVO:
        print("Envio de email: STAND-BY (aguardando credenciais SMTP)")
    print("Dashboard: STAND-BY (aguardando definição da diretoria)")
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

    enviar = PythonOperator(
        task_id='enviar_email',
        python_callable=enviar_email,
    )

    notificar = PythonOperator(
        task_id='notificar_conclusao',
        python_callable=notificar_conclusao,
    )

    fim = EmptyOperator(task_id='fim')

    # Ordem de execução
    inicio >> relatorio_individual >> relatorio_consolidado >> validar >> enviar >> notificar >> fim
