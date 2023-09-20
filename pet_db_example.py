import datetime
import os

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id='pet_table_pg',
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:
    
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        postgres_conn_id="postgres_localhost",
        sql="sql/pet_schema.sql",
    )

    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_localhost",
        sql="sql/pet_schema.sql",
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id="postgres_localhost",
        sql="SELECT * FROM pet;",
    )

    get_birth_date = PostgresOperator(
        task_id="get_birth_date",
        postgres_conn_id="postgres_localhost",
        sql="SELECT * FROM pet WHERE birth_date BETWEEN SYMMETRIC %(begin_date)s AND %(end_date)s",
        parameters={"begin_date": "2020-01-01", "end_date": "2020-12-31"},
    )

    create_pet_table >> populate_pet_table >> get_all_pets >> get_birth_date
