# Dockerfile customizado para Airflow com ODBC SQL Server
FROM apache/airflow:2.8.1-python3.11

USER root

# Instalar dependências do sistema e driver ODBC Microsoft
RUN apt-get update && apt-get install -y \
    curl \
    gnupg2 \
    unixodbc \
    unixodbc-dev \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/11/prod.list > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql17 \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Instalar dependências Python
RUN pip install --no-cache-dir \
    pyodbc \
    pandas \
    openpyxl \
    python-dotenv
