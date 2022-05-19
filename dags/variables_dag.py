from datetime import datetime
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


def fetch():
    secret = Variable.get("data_secret", deserialize_json=True)
    print(secret)
    url = Variable.get("url_api")
    print(url)
    data_demo = Variable.get("data_demo", deserialize_json=True)
    print(f"id={data_demo['id']} name={data_demo['name']} path={data_demo['path']}")
    print("fetching data...")


def save():
    print("save data")
    return True


def notify(ti):
    save = ti.xcom_pull(key="return_value", task_ids="save_data")
    if bool(save):
        print("send notification")
    else:
        print("there is a problem we can not save the notification")


with DAG(
    dag_id="Variables",
    description="A Dag Demo",
    start_date=datetime(2022, 5, 16),
    schedule_interval="*/20 * * * *",
    catchup=False,
    tags=["variables"]
) as dag:

    fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch)
    clean_data = BashOperator(task_id="clean_data", bash_command="exit 0")
    save_data = PythonOperator(task_id="save_data", python_callable=save)
    send_notification = PythonOperator(task_id="notify", python_callable=notify)

    fetch_data >> clean_data >> save_data >> send_notification
