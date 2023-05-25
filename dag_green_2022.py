from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta, date

# Define DAG
default_args = {
    'owner': 'ahyar',
    'start_date': datetime(2023, 5, 24),
    'retries': 0,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'dag_green_taxi',
    default_args=default_args,
    description='End to end mini project',
    schedule='@daily' # Run everyday
)

task_1 = BashOperator(
    task_id='staging',
    bash_command='python3 /home/ahyar/green_2022.py',
    dag=dag,
)

task_2 = BashOperator(
    task_id='make_dwh_and_store_in_hive',
    bash_command='python3 /mnt/c/Users/user/Documents/airflow/dags/end_to_end.py',
    dag=dag,
)
# set task in order
task_1 >> task_2 