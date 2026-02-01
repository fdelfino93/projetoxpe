# Projeto Aplicado - Pipeline ETL de Relatórios

Pipeline automatizado para geração e consolidação de relatórios semanais de inadimplência.

## Objetivo

Automatizar a geração de relatórios de boletos e inadimplência que anteriormente era feita manualmente (3-4 horas/semana), reduzindo para execução automática em poucos minutos.

## Tecnologias

- **Python 3.10+** - Scripts de extração e transformação
- **Apache Airflow** - Orquestração e agendamento
- **Docker** - Containerização do Airflow
- **SQL Server** - Banco de dados fonte
- **Pandas** - Manipulação de dados
- **OpenPyXL** - Geração de relatórios Excel

## Estrutura do Projeto

```
projetoxpe/
├── airflow/
│   ├── dags/
│   │   └── relatorios_semanais.py    # DAG agendada para segunda 06:00
│   └── docker-compose.yaml           # Configuração do Airflow
├── Sprint1/                          # Entrega Sprint 1
│   ├── Area_de_Experimentacao.xlsx
│   ├── Relatorio_Projeto_Aplicado.docx
│   └── evidencias/
├── Sprint2/                          # Entrega Sprint 2
│   ├── Area_de_Experimentacao.xlsx
│   ├── Relatorio_Projeto_Aplicado.docx
│   └── evidencias/
├── extrair_relatorio.py              # Relatório individual por condomínio
├── extrair_consolidado.py            # Relatório consolidado geral
├── testar_conexao.py                 # Teste de conexão com SQL Server
├── condominios.xlsx                  # Lista de condomínios a processar
└── requirements.txt                  # Dependências Python
```

## Instalação

```bash
# Instalar dependências
pip install -r requirements.txt

# Configurar variáveis de ambiente (.env)
DB_SERVER=seu_servidor
DB_DATABASE=sua_base
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
```

## Execução

### Scripts Python
```bash
# Testar conexão
python testar_conexao.py

# Gerar relatório individual
python extrair_relatorio.py

# Gerar relatório consolidado
python extrair_consolidado.py
```

### Apache Airflow
```bash
cd airflow
docker-compose up -d

# Acessar: http://localhost:8080
# Login: admin / admin
```

## Relatórios Gerados

### Relatório Individual (por condomínio)
- **Aba Detalhado**: Todos os boletos com status de pagamento
- **Aba Resumo por Unidade**: Visão consolidada por unidade (Adimplente/Inadimplente)

### Relatório Consolidado
- **Aba Resumo Geral**: Métricas totais (unidades, valores, inadimplência)
- **Aba Por Condomínio**: Breakdown por condomínio
- **Aba Inadimplentes**: Lista detalhada de unidades em aberto

## Sprints

| Sprint | Entrega | Descrição |
|--------|---------|-----------|
| 1 | 25/01/2026 | Conexão SQL Server + Script de extração |
| 2 | 01/02/2026 | Relatórios melhorados + Airflow + Docker |
| 3 | 08/02/2026 | Integração final + Testes |

## Autor

Fernando Manoel Rodrigues Delfino

---
*Projeto Aplicado - Pós-Graduação em Arquitetura e Engenharia de Dados - XP Educação*
