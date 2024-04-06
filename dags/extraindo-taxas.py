from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime

with DAG(
    'Executando-notebook-etl',
    start_date=datetime(2024, 4, 4),
    schedule_interval="0 9 * * *" # Todos os dias as 9 da manhã
    ) as dag_executando_notebook_extracao:

        extraindo_dados = DatabricksRunNowOperator(
            task_id = 'extraindo-conversoes',
            databricks_conn_id = 'databricks_default',
            job_id = 223906880273094,
            notebook_params={"data_execucao": '{{data_interval_end.strftime("%Y-%m-%d")}}'}
        )

        transformando_dados = DatabricksRunNowOperator(
            task_id = 'transformando-dados',
            databricks_conn_id = 'databricks_default',
            job_id = 1012410184467890
        )

        enviando_relatorio = DatabricksRunNowOperator(
            task_id = 'enviando-relatorio',
            databricks_conn_id = 'databricks_default',
            job_id = 876511199334532
        )

        extraindo_dados >> transformando_dados >> enviando_relatorio