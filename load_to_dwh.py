from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime
import os
import yaml

from functions.dshop_processing_func import load_to_dwh

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}



def return_tables():
    this_folder = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(this_folder,'configs','config.yaml'),'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config.get('daily_etl').get('sources').get('loadtodwh')
        
        
def load_to_dwh_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_dwh",
        python_callable=load_to_dwh,
        op_kwargs={"table": value},
        provide_context=True,
    )
        
dag = DAG(
    dag_id="load_to_dwh",
    description="Load data from silver to dwh",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 8, 11)
)

dummy1 = DummyOperator(
    task_id='start_load_to_dwh',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_load_to_dwh',
    dag=dag
)

for table in return_tables():
    dummy1 >> load_to_dwh_group(table) >> dummy2