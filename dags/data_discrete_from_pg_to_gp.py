import logging
from airflow import DAG
import pendulum
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models.baseoperator import chain
from airflow.sensors.external_task import ExternalTaskSensor
import psycopg2
from psycopg2 import sql
from datetime import timedelta

# конфиг DAG
OWNER = "kudryash"
DAG_ID = "data_discrete_from_pg_to_gp"

# Используемые БД в DAG (В ms sql и в pg названия БД одинаковые)
DB_NAME = "electro_abramovo"

# Пароли от БД
PG_PASSWORD = Variable.get("pg_password")
GP_PASSWORD = Variable.get("gp_password")

LONG_DESCRIPTION = """
Архивные данные за вчерашний день грузятся из БД объекта в хранилище GP
"""

SHORT_DESCRIPTION = "Архив из pg в GP"

args = {
    "owner": OWNER,
    "start_date": pendulum.datetime(2025, 10, 26, tz='Europe/Moscow'),
    "catchup": True,
    "retries": 3,
    "retry_delay": pendulum.duration(hours=1)
}


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def transfer_discrete_data_from_pg_to_gp(sub_sys, **context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load data {sub_sys} for dates: {start_date}/{end_date}")
    conn_pg = psycopg2.connect(host="postgres_db", database=DB_NAME, user="postgres", password=PG_PASSWORD)
    table_name = start_date.replace('-', '_')
    conn_gp = psycopg2.connect(host="greenplum-db", database=DB_NAME, user="gpadmin", password=GP_PASSWORD)

    try:
        with conn_pg.cursor() as cursor:
            cursor.execute(sql.SQL("""
                CREATE TABLE IF NOT EXISTS {data_table} AS
                SELECT * FROM {event_table}
                UNION
                SELECT "time", "Mcs", num_sign, "data", "Pr", bstate, bsrc, kks_id_signal
                FROM {state_table};
                """).format(
                data_table=sql.Identifier(f"public.data_{sub_sys}_{table_name}"),
                event_table=sql.Identifier(f"tmp_{sub_sys}_{table_name}_event"),
                state_table=sql.Identifier(f"public.tmp_{sub_sys}_{table_name}_state")
            )
                           )
        conn_pg.commit()
        logging.info(f"Таблица public.data_{sub_sys}_{table_name} для сбора данных {sub_sys} подсистемы создана")

        with conn_gp.cursor() as cursor:
            cursor.execute(sql.SQL("""
            DROP EXTERNAL TABLE IF EXISTS {ext_data_table};
             CREATE READABLE EXTERNAL TABLE {ext_data_table} (
                "time" int4,
                "Mcs" int4,
                num_sign int4,
                "data" float4,
                "Pr" int2,
                bstate int2,
                bsrc int2,
                kks_id_signal bpchar(25)
            )
            LOCATION('pxf://{pg_table}?PROFILE=jdbc&SERVER=electro_abramovo_server')
            FORMAT 'CUSTOM' (FORMATTER='pxfwritable_import');

            INSERT INTO {data_table}
            SELECT * FROM {ext_data_table};   
            """).format(ext_data_table=sql.Identifier(f"ext_data_{sub_sys}"),
                        data_table=sql.Identifier(f"public.data_{sub_sys}"),
                        pg_table=sql.Identifier(f"public.data_{sub_sys}_{table_name}")
                        )
                           )
        conn_gp.commit()
        logging.info(f"Перекачка данных из public.data_{sub_sys}_{table_name} в хранилище завершена")

        with conn_pg.cursor() as cursor:
            cursor.execute(sql.SQL("DROP TABLE IF EXISTS {};").format(
                sql.Identifier(f"public.data_{sub_sys}_{table_name}")
            ))
        conn_pg.commit()
        logging.info(f"Таблица public.data_{sub_sys}_{table_name} дропнута")
    finally:
        conn_pg.close()
        conn_gp.close()


with DAG(
        dag_id=DAG_ID,
        schedule_interval='0 5 * * *',
        default_args=args,
        tags=['pg', 'gp'],
        description=SHORT_DESCRIPTION,
        concurrency=1,
        max_active_tasks=1,
        max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id='start',
    )

    sensor_on_transfer_data_16_ms_pg = ExternalTaskSensor(
        task_id="sensor_on_transfer_data_16_ms_pg",
        external_dag_id="data_16_from_mssql_to_pg",
        allowed_states=["success"],
        mode="reschedule",
        timeout=36000,  # длительность работы сенсора
        poke_interval=60,  # частота проверки
    )

    transfer_data_16_from_pg_to_gp = PythonOperator(
        task_id='transfer_data_16_from_pg_to_gp',
        python_callable=transfer_discrete_data_from_pg_to_gp,
        op_kwargs={'sub_sys': 16},
        execution_timeout=timedelta(minutes=30)
    )

    transfer_data_85_from_pg_to_gp = PythonOperator(
        task_id='transfer_data_85_from_pg_to_gp',
        python_callable=transfer_discrete_data_from_pg_to_gp,
        op_kwargs={'sub_sys': 85},
        execution_timeout=timedelta(minutes=30)
    )

    end = EmptyOperator(
        task_id='end'
    )

    chain(
        start,
        sensor_on_transfer_data_16_ms_pg,
        transfer_data_16_from_pg_to_gp,
        transfer_data_85_from_pg_to_gp,
        end
    )