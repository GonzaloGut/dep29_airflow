from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import requests

default_args = {
    'owner': 'gonzalo',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='04_ejercicio_01',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=days_ago(1),
    tags=['04_ejercicio', 'ingestion', 'api', 'postgres','ejercicio'],
    catchup=False
)
def ingestion_flow():

    @task()
    def fetch_users_data():
        url = "https://jsonplaceholder.typicode.com/users"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def save_users_extended(users: list):
        if not users:
            return

        pg_hook = PostgresHook(postgres_conn_id='postgres_datapath')
        
        create_sql = """
        CREATE TABLE IF NOT EXISTS users_extended (
            id INT PRIMARY KEY,
            name VARCHAR(255),
            username VARCHAR(255),
            email VARCHAR(255),
            street VARCHAR(255),
            city VARCHAR(255),
            zipcode VARCHAR(255),
            lat VARCHAR(50),
            lng VARCHAR(50),
            phone VARCHAR(255),
            website VARCHAR(255),
            company_name VARCHAR(255)
        );
        """
        pg_hook.run(create_sql)
        
        pg_hook.run("TRUNCATE TABLE users_extended")

        rows_to_insert = []
        for u in users:
            address = u.get('address', {})
            geo = address.get('geo', {})
            company = u.get('company', {})
            
            row = (
                u.get('id'),
                u.get('name'),
                u.get('username'),
                u.get('email'),
                address.get('street'),
                address.get('city'),
                address.get('zipcode'),
                geo.get('lat'),
                geo.get('lng'),
                u.get('phone'),
                u.get('website'),
                company.get('name')
            )
            rows_to_insert.append(row)

        pg_hook.insert_rows(
            table='users_extended',
            rows=rows_to_insert,
            target_fields=['id', 'name', 'username', 'email', 'street', 'city', 'zipcode', 'lat', 'lng', 'phone', 'website', 'company_name']
        )
        print(f"Insertados {len(rows_to_insert)} usuarios.")


    trigger_transform = TriggerDagRunOperator(
        task_id='trigger_transform_dag',
        trigger_dag_id='04_ejercicio_02',
        wait_for_completion=False
    )

    data = fetch_users_data()
    saved = save_users_extended(data)
    saved >> trigger_transform

dag_instance = ingestion_flow()

