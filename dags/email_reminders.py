from datetime import datetime
from airflow import DAG
from airflow.operators.dummy import DummyOperator


with DAG(
    dag_id="Email_Reminders",
    description="A Dag Roles / Permissions",
    start_date=datetime(2022, 5, 18),
    schedule_interval="*/2 * * * *",
    catchup=False,
    tags=["roles"]
) as dag:

    fetch_data = DummyOperator(task_id="fetch_data")
    clean_data = DummyOperator(task_id="clean_data")
    save_data = DummyOperator(task_id="save_data")
    notify = DummyOperator(task_id="notify")

    fetch_data >> clean_data >> save_data >> notify
