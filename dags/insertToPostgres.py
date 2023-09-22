from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

import os
from random import randint
import logging



LOGGER = logging.getLogger(__name__)

airflow_home = os.getenv("AIRFLOW_HOME")
# global var

def insert_to_pg(**kwargs):
    timestamp_now = datetime.now().timestamp()
    query = f"""
INSERT INTO public.transaction(id, accountid) VALUES ({timestamp_now}, {str(randint(1,50))});
"""

    insert_app = PostgresOperator(
        task_id="create_pet_table",
        sql=query,
        postgres_conn_id="postgres_app"
    )

    insert_app.execute(kwargs)


dag = DAG('insert_to_pg', description='Hello World DAG',
          schedule_interval=None,
          start_date=datetime(2023, 6, 10), catchup=True)

start_task = DummyOperator(task_id='start_task', dag=dag)
insert_to_pg = PythonOperator(
    task_id = "insert_to_pg",
    python_callable = insert_to_pg,
    dag = dag
)

start_task >> insert_to_pg