from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from youtube_etl import yt_etl

default_args ={
    'owner':"airflow",
    "depends_on_past":False,
    'start_date':datetime(2023,12,18),
    'email':{"ankitmahto75@gmail.com"},
    'email_on_failure':False,
    'retries':1,
    'retry_delay':timedelta(minutes=1)
}

dag = DAG(
    'yt_dag',
    default_args=default_args,
    description="First etl"
)

run_etl = PythonOperator (
    task_id= "complete_yt_etl",
    python_callable=yt_etl,
    dag=dag
)