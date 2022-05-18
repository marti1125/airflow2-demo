from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    dag_id="Demo01", 
    description="A Dag Demo for Coffe & Learn", 
    start_date=datetime(2022, 5, 16),
    schedule_interval="*/3 * * * *",
    catchup=False,
    tags=["demo", "coffe & learn"],
    access_control={
        "data_engineer": {"can_read", "can_edit"}
    }) as dag:
    
    fetch_data = DummyOperator(task_id="fetch_data")
    clean_data = DummyOperator(task_id="clean_data")
    save_data = DummyOperator(task_id="save_data")
    notify = DummyOperator(task_id="notify")

    fetch_data >> clean_data >> save_data >> notify
