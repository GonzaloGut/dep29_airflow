
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils.dates import days_ago
from datetime import timedelta, datetime
import pandas as pd

default_args = {
    'owner': 'gonzalo',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id='04_ejercicio_02',
    default_args=default_args,
    schedule_interval=None,
    start_date=days_ago(1),
    tags=['04_ejercicio', 'etl', 'bigquery','ejercicio'],
    catchup=False
)
def transform_load_flow():

    @task()
    def extract_extended_data():
        """
        Lee datos raw de Postgres users_extended
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_datapath')
        df = pg_hook.get_pandas_df("SELECT * FROM users_extended")
        return df.to_dict(orient='records')

    @task()
    def apply_business_logic(data_list: list):
        """
        Reglas de Negocio:
        1. Clasificar latitud (Hemisferio Norte/Sur)
        2. Limpiar Website (quitar http/https si existe para estandarizar)
        3. Email Domain extraction
        """
        processed = []
        for row in data_list:
            new_row = row.copy()
            
            try:
                lat = float(new_row.get('lat', 0))
                new_row['hemisphere'] = 'North' if lat >= 0 else 'South'
            except:
                new_row['hemisphere'] = 'Unknown'

            website = new_row.get('website', '')
            new_row['website_clean'] = website.replace('http://', '').replace('https://', '')

            new_row['etl_loaded_at'] = datetime.now().isoformat()
            
            processed.append(new_row)
            
        return processed

    @task()
    def load_to_bigquery_final(data: list):
        """
        Carga final a BigQuery
        """
        if not data:
            print("No data to load")
            return

        PROJECT_ID = 'test-gonzalo-1' 
        DATASET_ID = 'datapath_dataset'
        TABLE_ID = 'users_analytics_final'
        
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        bq_hook.insert_all(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            rows=data
        )
        print(f"Cargados {len(data)} registros en {TABLE_ID}")

    @task()
    def create_bq_table():
        """
        Crea la tabla en BigQuery si no existe
        """
        from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
        
        PROJECT_ID = 'test-gonzalo-1' 
        DATASET_ID = 'datapath_dataset'
        TABLE_ID = 'users_analytics_final'
        
        bq_hook = BigQueryHook(gcp_conn_id='google_cloud_default')
        
        schema = [
            {"name": "id", "type": "INTEGER", "mode": "REQUIRED"},
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "username", "type": "STRING", "mode": "NULLABLE"},
            {"name": "email", "type": "STRING", "mode": "NULLABLE"},
            {"name": "street", "type": "STRING", "mode": "NULLABLE"},
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "zipcode", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lat", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "lng", "type": "FLOAT", "mode": "NULLABLE"},
            {"name": "phone", "type": "STRING", "mode": "NULLABLE"},
            {"name": "website", "type": "STRING", "mode": "NULLABLE"},
            {"name": "company_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "hemisphere", "type": "STRING", "mode": "NULLABLE"},
            {"name": "website_clean", "type": "STRING", "mode": "NULLABLE"},
            {"name": "etl_loaded_at", "type": "TIMESTAMP", "mode": "NULLABLE"}
        ]
        
        bq_hook.create_empty_table(
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            schema_fields=schema,
            exists_ok=True
        )
        print(f"Tabla {TABLE_ID} verificada/creada")

    create_table_task = create_bq_table()
    raw_users = extract_extended_data()
    transformed = apply_business_logic(raw_users)
    load_task = load_to_bigquery_final(transformed)
    
    create_table_task >> load_task

dag_instance = transform_load_flow()
