from datetime import datetime, timedelta
from socket import timeout
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator


default_args = {
    "retry": 5,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email_on_retry": True,
    "email": "testing@mail.com",
}


def _downloading_data():
    with open("/tmp/my_file.txt", "w") as f:
        f.write("hello!")


def _checking_data():
    print("checking data... ")


def _failure(context):
    print("On call failure")
    print(context)


with DAG(
    dag_id="Sensor",
    description="A Dag Roles",
    start_date=datetime(2022, 5, 18),
    schedule_interval="*/5 * * * *",
    catchup=False,
    tags=["sensor"],
) as dag:

    downloading_data = PythonOperator(task_id="downloading_data", python_callable=_downloading_data,)

    checking_data = PythonOperator(task_id="checking_data", python_callable=_checking_data,)

    waiting_for_date = FileSensor(task_id="waiting_for_date", fs_conn_id="fs_default", filepath="my_file.txt", timeout=10)

    processing_data = BashOperator(task_id="processing_data", bash_command="exit 0", on_failure_callback=_failure,)

    downloading_data >> checking_data >> waiting_for_date >> processing_data