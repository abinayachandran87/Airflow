from airflow import DAG

from airflow.utils.dates import days_ago

from airflow.operators.python_operator import PythonOperator

from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator


def print_success():

    print('Successfully updated table')
    
# Define the DAG

dag = DAG(

    'load_table_s3',

    default_args={'start_date': days_ago(1)},

    schedule_interval='0 21 * * *',

    catchup=False

)


# Define the Task

load_table_s3 = SnowflakeOperator(

    task_id='load_table_s3',

    sql='./sqls/load_from_s3.sql',

    snowflake_conn_id='snowflake_conn',

    dag=dag

)

copy_into_table = SnowflakeOperator(
    task_id='copy_into_table',
    sql='./sqls/copy_into_table_from_s3.sql',
    snowflake_conn_id='snowflake_conn',
    dag=dag,
)

print_success = PythonOperator(

    task_id='print_success',

    python_callable=print_success,

    dag=dag

)




# Define the Dependencies

load_table_s3 >> copy_into_table >> print_success
