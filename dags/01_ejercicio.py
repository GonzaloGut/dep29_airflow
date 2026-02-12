from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime

default_args = {
    'owner': 'gonzalo',
    'depends_on_past': False,
    'start_date': days_ago(3),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    '01_ejercicio', 
    default_args=default_args, 
    schedule_interval='@daily',
    tags=['ejercicio']
    ) as dag:

    start = EmptyOperator(task_id='start')
    task_1 = DummyOperator(task_id='task_1')
    task_2 = DummyOperator(task_id='task_2')
    task_3 = DummyOperator(task_id='task_3')
    end = EmptyOperator(task_id='end')

    start >> task_1 >> task_2
    task_2 >> task_3 >> end