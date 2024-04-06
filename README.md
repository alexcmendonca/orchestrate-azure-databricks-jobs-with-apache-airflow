# Orchestrate Azure Databricks jobs with Apache Airflow

## 💡Objetivos
Projeto visa fornecer suporte à empresas que importam produtos de diferentes países, ajudando-a a ter uma previsão mais precisa dos gastos. Para isso, é essencial acompanhar as flutuações das moedas relevantes para a empresa, incluindo o Dólar Americano (USD), Libra Esterlina (GBP) e Euro (EUR). Nesse contexto, propõe-se o desenvolvimento de uma solução capaz de extrair diariamente as cotações dessas moedas, consolidando-as em uma tabela e em gráficos de fácil interpretação.

Para atingir esse objetivo, o projeto realizará a construção e execução de um pipeline completo, utilizando as capacidades do Apache Airflow e do Databricks na nuvem Azure. A integração com a plataforma de comunicação interna Slack será aproveitada para o envio automático da tabela e dos gráficos, garantindo uma comunicação eficiente e centralizada dentro da equipe.

###### Imagem 1: Grid View - Visualização da execução do DAG
<img src="/img/001-airflow-grid">


## 🖥️Desafios do Projeto
Fazer o uso do Databricks em conjunto com o Spark e a API do Pandas Spark para efetuar a leitura, manipulação e extração diária dos dados de cotação das moedas, criação de fluxos de trabalho (Workflows), implementado um job individual para cada notebook. O Apache Airflow para orquestrar e agendar a execução desses notebooks no ambiente do Azure Databricks. Esta abordagem integrada aproveita as potentes funcionalidades de ferramentas como PySpark, Airflow, Databricks e as requisições de API de taxa de câmbio, como a "Exchange Rates Data API - APILayer". E por último, criar um bot do Slack, utilizando sua API, para receber dados com a tabela de câmbio e gráficos do Databricks/Airflow.

###### Imagem 2: Fluxo de trabalho do Databricks
<img src="/img/002-workflow-databricks.png">


## 📄Conhecimentos Desenvolvidos
|Atividades|Realizadas |
|----------|-----------|
| Identificar como instalar o Airflow | Compreender e acessar o Azure Databricks |
| Reconhecer e controlar os gastos da Azure | Identificar e acessar a API “Exchange Rates Data API” | 
| Fazer a requisição dos dados para a API | Organizar esquematizar o projeto conforme a arquitetura medalhão | 
| Reconhecer e salvar os dados no formato Parquet | Orquestrar o notebook no Databricks com o Airflow | 
| Juntar os dados extraídos em somente um PySpark DataFrame | Filtrar os dados requeridos | 
| Transformar o DataFrame | Salvar os dados transformados no formato CSV | 
| Orquestrar com o Airflow os notebooks desenvolvidos no Databricks | Criar um Workspace no Slack | 
| Criar um Bot do Slack | Enviar arquivos CSV para o Workspace através do bot do Slack | 
| Enviar as imagens para o Workspace através do bot do Slack | Como trabalhar simultaneamente com o Airflow e o Databricks | 

##  🗂️Organização dos Arquivos

* notebooks: Arquivos desenvolvidos dentro do Databricks para manipulação dos dados: extração, transformação e automatização de relatórios para envio no Slack
* dags: Arquivos responsáveis por definir e executar Directed Acyclic Graphs (DAGs). Esses arquivos realizam um conjunto de tarefas para extrair dados do mercado financeiro sobre diferentes ações da bolsa de valores.

##  🎞️Imagens do Projeto

###### Imagem 3: Microsoft Azure - Serviços
<img src="/img/003-azure.png">

###### Imagem 4: Databricks Jobs
<img src="/img/004-job.png">

###### Imagem 5: Implementando um bot Slack para receber dados orquestrados através da integração entre Databricks e Airflow.
<img src="/img/005-slack.png">


## 🔍Referências
- [Alura](https://www.alura.com.br/)