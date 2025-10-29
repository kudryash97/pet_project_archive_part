import logging
from airflow import DAG
import pendulum
from airflow.models import Variable
import pandas as pd
import random
from sqlalchemy import create_engine
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta
import psycopg2
import csv
import io

#конфиг DAG
OWNER = "kudryash"
DAG_ID = "data_10_from_mssql_to_pg"


# Используемые БД в DAG (В ms sql и в pg названия БД одинаковые)
DB_NAME = "electro_abramovo"

# Пароли от БД
MS_SQL_PASSWORD = Variable.get("ms_password")
PG_PASSWORD=Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "Архивные данные 10 из ms sql грузятся в postgres"

args = {
    "owner" : OWNER,
    "start_date" : pendulum.datetime(2025, 10, 26, tz='Europe/Moscow'),
    "catchup" : True,
    "retries" : 3,
    "retry_delay" : pendulum.duration(hours=1)
}

def gen_float(min_val, max_val, decimal_places=4):
    number = random.uniform(min_val, max_val)
    return round(number, decimal_places)

def gen_value(val : str) -> float:
    value_ranges = {
        'А': (335, 346),
        'кВ': (-280, 280),
        'Гц': (49.8, 50.2),
        'МВт': (170, 220),
        'МВА': (450, 580),
        'МВАр': (-180, 150)
    }
    if val in value_ranges:
        min_val, max_val = value_ranges[val]
        return gen_float(min_val, max_val)
    else:
        return gen_float(10, 20)


def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date



def transfer_data_10_from_ms_to_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"Start load for dates: {start_date}/{end_date}")
    engine = create_engine(f"mssql+pymssql://sa:{MS_SQL_PASSWORD}@sql-server/{DB_NAME}?charset=utf8")

    sub_sys_value = 10
    pattern = "%EN6%"

    sql_query = """
    SELECT kks_id_signal, signal_indx, dimension
    FROM dbo.Link_signals
    WHERE kks_id_signal LIKE %(pattern)s 
      AND kks_id_param IS NOT NULL  
      AND sub_sys = %(sub_sys_value)s;
    """

    df_signals = pd.read_sql(sql=sql_query, con=engine, params={"pattern": pattern, "sub_sys_value": sub_sys_value})
    logging.info(f"Найдено {len(df_signals)} сигналов 10 подсистемы")

    seconds = pd.date_range(start=f"{start_date} 00:00:00", end=f"{start_date} 23:59:59", freq='s')
    time_unix = (seconds.astype(int) // 10 ** 9).values


    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    logging.info(f"Генерация CSV файла для {len(df_signals)} сигналов × {len(time_unix)} меток времени")

    total_rows = 0
    for _, signal in df_signals.iterrows():
        for timestamp in time_unix:
            csv_writer.writerow([
                timestamp,  # time
                476000,  # Mcs
                signal['signal_indx'],  # num_sign
                gen_value(signal['dimension']),  # data
                0,  # bzone
                1,  # isevnt
                1,  # bstate
                0,  # bsrc
                signal['kks_id_signal']  # kks_id_signal
            ])
            total_rows += 1

            if total_rows % 1000000 == 0:
                logging.info(f"Generated {total_rows:,} rows")

    table_name = f"tmp_10_{start_date.replace('-', '_')}"

    conn = psycopg2.connect(host="postgres_db", database=DB_NAME, user="postgres", password=PG_PASSWORD)

    try:
        with conn.cursor() as cursor:
            cursor.execute(f"""
                    CREATE TABLE IF NOT EXISTS {table_name}_event (
                        time int,
                        Mcs int,
                        num_sign int, 
                        data real,
                        bzone smallint,
                        isevnt smallint,
                        bstate smallint,
                        bsrc smallint,
                        kks_id_signal CHAR(25)
                    );
                """)

            csv_buffer.seek(0)
            cursor.copy_expert(f"""
                    COPY {table_name}_event (time, Mcs, num_sign, data, bzone, isevnt, bstate, bsrc, kks_id_signal)
                    FROM STDIN WITH CSV
                """, csv_buffer)

            logging.info(f"Копирование завершено: {total_rows:,} строк")

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_time AS 
                SELECT DISTINCT time AS time_page
                FROM {table_name}_event
                WHERE time % 5 = 0;
            """)

            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name}_state AS 
                SELECT time AS time_page, time, Mcs, num_sign, data, bzone, isevnt, bstate, bsrc, kks_id_signal
                FROM {table_name}_event
                WHERE time % 5 = 0;
            """)
            cursor.execute(f"""
                CREATE TABLE IF NOT EXISTS tmp_80_{start_date.replace('-', '_')}_time AS
                SELECT time_page
                FROM {table_name}_time;
""")

            conn.commit()
            logging.info(f"Все 3 таблицы 10 подсистемы созданы")


    finally:
        conn.close()


    logging.info(f"Download for date success: {start_date}")

with DAG(
    dag_id=DAG_ID,
    schedule_interval='0 5 * * *',
    default_args=args,
    tags=['ms_sql', 'pg'],
    description=SHORT_DESCRIPTION,
    concurrency=1,
    max_active_tasks=1,
    max_active_runs=1
) as dag:
    dag.doc_md = LONG_DESCRIPTION

    start = EmptyOperator(
        task_id='start',
    )

    transfer_data_10_from_ms_to_pg = PythonOperator(
        task_id='transfer_data_10_from_ms_to_pg',
        python_callable=transfer_data_10_from_ms_to_pg,
        execution_timeout=timedelta(minutes=30)
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> transfer_data_10_from_ms_to_pg >> end