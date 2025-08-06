from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

def saludar():
    print("¡Este es mi DAG local!")
    return "Completado"

def procesar_datos():
    print("Procesando datos para Cloud Composer...")
    return "Datos procesados"

default_args = {
    'owner': 'gvillafane',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_de_prueba',
    default_args=default_args,
    description='Mi primer DAG para Cloud Composer',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['ejemplo', 'local', 'corebi'],
)

tarea_saludo = PythonOperator(
    task_id='saludar',
    python_callable=saludar,
    dag=dag,
)

tarea_procesar = PythonOperator(
    task_id='procesar_datos',
    python_callable=procesar_datos,
    dag=dag,
)

# Definir dependencias
tarea_saludo >> tarea_procesar