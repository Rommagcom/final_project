from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, date
import os
import yaml

from functions.dshop_processing_func import load_from_api, load_from_db,load_to_silver,load_to_dwh

default_args = {
    "owner": "airflow",
    "email_on_failure": False
}

def return_dates():
    return [
        datetime.now()
        ]

def return_tables(source):
    this_folder = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(this_folder,'configs','config.yaml'),'r') as yaml_file:
        config = yaml.safe_load(yaml_file)
        return config.get('daily_etl').get('sources').get(source)

def load_to_bronze_out_of_stock(for_date):
    return PythonOperator(
        task_id="load_for_"+for_date+"_to_bronze",
        python_callable=load_from_api,
        op_kwargs={"load_for_date": for_date},
        provide_context=True,
    )

def load_to_bronze_db_tables(value):
    return PythonOperator(
        task_id="load_"+value+"_to_bronze",
        python_callable=load_from_db,
        op_kwargs={"table": value},
        provide_context=True,
    )

def load_to_silver_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_silver",
        python_callable=load_to_silver,
        op_kwargs={"table": value},
        provide_context=True,
    )

def load_to_dwh_group(value):
    return PythonOperator(
        task_id="load_"+value+"_to_dwh",
        python_callable=load_to_dwh,
        op_kwargs={"table": value},
        provide_context=True,
    )

dag = DAG(
    dag_id="dshop_etl_processing",
    description="DShop data processing, getting data from db, api -> cleaning, loading to silver and DWH",
    schedule_interval="0 0 * * *",
    start_date=datetime(2021, 8, 24)
)

dummy1 = DummyOperator(
    task_id='start_loading_to_bronze',
    dag=dag
)
dummy2 = DummyOperator(
    task_id='finish_loading_to_bronze',
    dag=dag
)

dummy3 = DummyOperator(
    task_id='start_loading_to_silver',
    dag=dag
)

dummy4 = DummyOperator(
    task_id='finish_loading_to_silver',
    dag=dag
)

dummy5 = DummyOperator(
    task_id='start_loading_to_dwh',
    dag=dag
)

dummy6 = DummyOperator(
    task_id='finish_loading_to_dwh',
    dag=dag
)


for call_date in return_dates():
    dummy1 >> load_to_bronze_out_of_stock(call_date.strftime("%Y-%m-%d")) >> dummy2

for table in return_tables('postgresql'):
    dummy1 >> load_to_bronze_db_tables(table) >> dummy2 >> dummy3

for table in return_tables('loadtosilver'):
    dummy3 >> load_to_silver_group(table) >> dummy4 >> dummy5

for table in return_tables('loadtodwh'):
    dummy5 >> load_to_dwh_group(table) >> dummy6

