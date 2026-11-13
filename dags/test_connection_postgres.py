
from airflow.decorators import dag, task
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import traceback


def test_postgres_connection():
    hook = PostgresHook(
        postgres_conn_id="pg_pedroprojects"
    )

    try:
        conn = hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("""
                    SELECT
                        current_database(),
                        current_user,
                        inet_server_addr(),
                        inet_server_port();
                """)
                print("Conexão OK:", cursor.fetchone())
        finally:
            conn.close()

    except Exception as exc:
        print("TIPO DO ERRO:", type(exc).__name__)
        print("REPRESENTAÇÃO DO ERRO:", repr(exc))
        print("BYTES DO ERRO:", repr(
            str(exc).encode("utf-8", errors="backslashreplace")
        ))
        traceback.print_exc()
        raise


@dag(
    dag_id="test_postgres_connection",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def test_connection():

    @task
    def test_connection_postgres():
        PythonOperator(
            task_id="test_connection",
            python_callable=test_postgres_connection,
        )
    test_connection_postgres()
test_connection()