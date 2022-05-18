from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    dag_id="Reminders", 
    description="A Dag Roles for Coffe & Learn", 
    start_date=datetime(2022, 5, 16),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["demo", "coffe & learn"],
    access_control={
        "data_engineer": {"can_read"},
        "team_lead": {"can_read", "can_edit", "can_delete"}
    }) as dag:
    
    fetch_data = DummyOperator(task_id="fetch_data")
    clean_data = DummyOperator(task_id="clean_data")
    save_data = DummyOperator(task_id="save_data")
    notify = DummyOperator(task_id="notify")

    fetch_data >> clean_data >> save_data >> notify
