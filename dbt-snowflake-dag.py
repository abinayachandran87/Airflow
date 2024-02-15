from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import os
DAG_ID = os.path.basename(__file__).replace(".py", "")
with DAG(dag_id=DAG_ID, schedule_interval=None, catchup=False, start_date=days_ago(1)) as dag:
    cli_command = BashOperator(
        task_id="bash_command",
        bash_command="cp -R /usr/local/airflow/dags/ /tmp;\
	cd /tmp/dbt_tpch;\
	/usr/local/airflow/.local/bin/dbt  run --project-dir /tmp/dbt_tpch/ --profiles-dir ..;\
	cat /tmp/dbt_logs/dbt.log"
    )