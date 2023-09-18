

# create_pet_table, populate_pet_table, get_all_pets, and get_birth_date are examples of tasks created by
# instantiating the Postgres Operator

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID")
DAG_ID = "postgres_operator_dag"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2020, 2, 2),
    schedule="@once",
    catchup=False,
) as dag:

    # SAMPLE USING SQL FILE
    create_pet_table = PostgresOperator(
        task_id="create_pet_table",
        sql="sql/birth_date.sql",
        postgres_conn_id="postgres_default"
    )

    # SAMPLE WITH SQL QUERY
    populate_pet_table = PostgresOperator(
        task_id="populate_pet_table",
        sql="""
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Max', 'Dog', '2018-07-05', 'Jane');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Susie', 'Cat', '2019-05-01', 'Phil');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Lester', 'Hamster', '2020-06-23', 'Lily');
            INSERT INTO pet (name, pet_type, birth_date, OWNER)
            VALUES ( 'Quincy', 'Parrot', '2013-08-11', 'Anne');
            """,
        postgres_conn_id="postgres_default"
    )

    get_all_pets = PostgresOperator(
        task_id="get_all_pets", 
        sql="SELECT * FROM pet;"
        postgres_conn_id="postgres_default"
    )

    create_pet_table >> populate_pet_table >> get_all_pets