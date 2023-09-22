import datetime

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="postgres_operator_dag",
    start_date=datetime.datetime(2020, 2, 2),
    schedule_interval="@once",
    catchup=False,
) as dag:
    
    # CREATE TABLE IF NOT EXISTS
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="""
            CREATE TABLE IF NOT EXISTS pet (
            pet_id SERIAL PRIMARY KEY,
            name VARCHAR NOT NULL,
            pet_type VARCHAR NOT NULL,
            birth_date DATE NOT NULL,
            OWNER VARCHAR NOT NULL);
          """,
        postgres_conn_id="postgres_default"
    )

    # INSERT TABLE
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        postgres_conn_id="postgres_default",
        sql="postgres_operator/pet_schema.sql",
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets",
        postgres_conn_id="postgres_default",
        sql="SELECT * FROM pet;",
    )

    create_pet_table >> populate_pet_table >> get_all_pets

 