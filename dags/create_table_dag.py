from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'Kulani',
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

def import_data_from_csv():
    with DAG(
        dag_id='dag_with_postgres_operator',
        default_args=default_args,
        start_date=datetime(2023, 12, 20),
        schedule_interval='0 0 * * *'
    ) as dag:
        create_table_task = PostgresOperator(
            task_id='create_postgres_table',
            postgres_conn_id='local_postgres',
            sql="""
            CREATE TABLE IF NOT EXISTS dag_runs (
                dt DATE,
                dag_id VARCHAR,
                PRIMARY KEY (dt, dag_id)
            )
            """
        )

        import_data_task_1 = PostgresOperator(
            task_id='import_data_from_csv_1',
            postgres_conn_id='local_postgres',
            sql="""
            COPY dag_runs FROM 'notebooks\track_table.csv' DELIMITER ',' CSV HEADER;
            """
        )

        import_data_task_2 = PostgresOperator(
            task_id='import_data_from_csv_2',
            postgres_conn_id='local_postgres',
            sql="""
            COPY dag_runs FROM 'notebooks\trajectory_table.csv' DELIMITER ',' CSV HEADER;
            """
        )

        create_table_task >> import_data_task_1
        create_table_task >> import_data_task_2

    return dag

dag = import_data_from_csv()