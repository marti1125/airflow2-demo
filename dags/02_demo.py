from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def fetch():
    print("fetching data...")


def save():
    print("save data")


def notify():
    print("send notification")


with DAG(
    dag_id="Demo02",
    description="A Dag Demo",
    start_date=datetime(2022, 3, 16),
    schedule_interval="*/2 * * * *",
    #catchup=True,
    tags=["demo"]
) as dag:

    fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch)
    clean_data = BashOperator(task_id="clean_data", bash_command="exit 0")
    save_data = PythonOperator(task_id="save_data", python_callable=save)
    send_notification = PythonOperator(task_id="notify", python_callable=notify)

    fetch_data >> clean_data >> save_data >> send_notification
