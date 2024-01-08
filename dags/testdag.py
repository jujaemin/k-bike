from airflow import DAG
from datetime import datetime
from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}
dag = DAG('testdag', default_args=default_args, schedule_interval='@daily')

dummy1 = DummyOperator(task_id='dummy1', dag=dag)

dummy1