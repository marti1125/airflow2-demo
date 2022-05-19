from datetime import datetime, timedelta

from airflow.decorators import task, dag

default_args = {
    "start_date": datetime(2022, 4, 1),
}


@task.python(task_id="new_getting_data", do_xcom_push=False, multiple_outputs=True)
def getting_data():
    print("getting data")
    # multiple return xcom by key
    return {"name": "hello", "path": "/path/demo"}


@task.python
def processing_data(name, path):
    print("processing data", name, path)


@task.python
def check():
    print("checking")


@dag(description="demo new way", schedule_interval="*/10 * * * *", 
    default_args=default_args, catchup=False, max_active_runs=1, tags=["taskflowapi"], 
    dagrun_timeout=timedelta(minutes=10))
def new_way_dag():

    data = getting_data()

    processing_data(data["name"], data["path"]) >> check()

dag = new_way_dag()
