from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.models.param import Param


def get_name(ti,**context):
    dag_params = context["params"]
    nombre_param = dag_params.get("nombre")
    apellido_param = dag_params.get("apellido")

    ti.xcom_push(key="nombre", value=nombre_param)
    ti.xcom_push(key="apellido", value=apellido_param)


def get_age(ti,**context):
    dag_params = context["params"]
    edad = dag_params.get("edad")

    ti.xcom_push(key="edad",value=edad)

def extract(ti):

    nombre = ti.xcom_pull(task_ids="get_name_task",key="nombre")
    apellido = ti.xcom_pull(task_ids="get_name_task",key="apellido")
    edad = ti.xcom_pull(task_ids="get_age_task",key="edad")

    print(f"Nombre: {nombre}, Apellido: {apellido}, Edad: {edad}")


default_args = {
    'owner': 'gonzalo',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


with DAG(
    '03_ejercicio',
    default_args=default_args,
    description='Demostrando xcoms',
    schedule_interval=None,
    tags=['ejercicio'],
    params={
        "nombre": Param("",type="string"),
        "apellido": Param("",type="string"),
        "edad": Param(18,type="integer", minimum=18, maximum=90)
    }
) as dag:

    taskA = PythonOperator(
        task_id="get_name_task",
        python_callable=get_name,
        provide_context=True
    )

    taskB = PythonOperator(
        task_id="get_age_task",
        python_callable=get_age,
        provide_context=True
    )

    taskC = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    [taskA,taskB] >> taskC
