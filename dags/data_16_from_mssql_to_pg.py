import logging
from airflow import DAG
import pendulum
from airflow.models import Variable
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

#конфиг DAG
OWNER = "kudryash"
DAG_ID = "data_16_from_mssql_to_pg"


# Используемые БД в DAG (В ms sql и в pg названия БД одинаковые)
DB_NAME = "electro_abramovo"

# Пароли от БД
MS_SQL_PASSWORD = Variable.get("ms_password")
PG_PASSWORD=Variable.get("pg_password")

LONG_DESCRIPTION = """
# LONG DESCRIPTION
"""

SHORT_DESCRIPTION = "Архивные данные из ms sql грузятся в postgres"

args = {
    "owner" : OWNER,
    "start_date" : pendulum.datetime(2025, 10, 26, tz='Europe/Moscow'),
    "catchup" : True,
    "retries" : 3,
    "retry_delay" : pendulum.duration(hours=1)
}

def get_dates(**context) -> tuple[str, str]:
    """"""
    start_date = context["data_interval_start"].format("YYYY-MM-DD")
    end_date = context["data_interval_end"].format("YYYY-MM-DD")

    return start_date, end_date

def transfer_data_16_from_ms_to_pg(**context):
    start_date, end_date = get_dates(**context)
    logging.info(f"💻 Start load for dates: {start_date}/{end_date}")
    engine = create_engine(f"mssql+pymssql://sa:{MS_SQL_PASSWORD}@sql-server/{DB_NAME}?charset=utf8")

    sub_sys_value = 16
    pattern = "%EN6%"
    engine_pg = create_engine(f'postgresql+psycopg2://postgres:{PG_PASSWORD}@postgres_db/{DB_NAME}')

    sql_query = """
    SELECT *
    FROM dbo.Link_signals
    WHERE kks_id_signal LIKE %(pattern)s 
      AND kks_id_param IS NOT NULL  
      AND sub_sys = %(sub_sys_value)s;
    """
    df = pd.read_sql(sql=sql_query, con=engine, params={"pattern": pattern, "sub_sys_value": sub_sys_value})
    hours = pd.date_range(start=f"{start_date} 00:00:00", end=f"{start_date} 23:00:00", freq="h")
    random_seconds = np.random.randint(0, 3600, 24)
    df_time = pd.DataFrame({
        "time_page": hours,
        "time": hours + pd.to_timedelta(random_seconds, unit="s"),
        "Mcs": 476000, "data": 2, "Pr": 2, "bstate": 1, "bsrc": 0}
    )
    df_sql = df[["signal_indx", "kks_id_signal"]].rename(columns={"signal_indx": "num_sign"})
    df_time["time_page"] = df_time["time_page"].astype(int) // 10 ** 9
    df_time["time"] = df_time["time"].astype(int) // 10 ** 9
    result = df_time.merge(df_sql, how="cross")
    cols_order = ["time_page", "time", "Mcs", "num_sign", "data", "Pr", "bstate", "bsrc", "kks_id_signal"]

    df_tmp_16_state = result[cols_order]
    df_tmp_16_time = df_time["time_page"]
    df_tmp_16_event = df_tmp_16_state[["time", "Mcs", "num_sign", "data", "Pr", "bstate", "bsrc", "kks_id_signal"]]
    df_tmp_16_state.to_sql(f"tmp_16_{start_date}_state".replace('-', '_'), con=engine_pg, if_exists='replace', index=False)
    df_tmp_16_time.to_sql(f"tmp_16_{start_date}_time".replace('-', '_'), con=engine_pg, if_exists='replace', index=False)
    df_tmp_16_event.to_sql(f"tmp_16_{start_date}_event".replace('-', '_'), con=engine_pg, if_exists='replace', index=False)
    logging.info(f"✅ Download for date success: {start_date}")

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

    transfer_data_16_from_ms_to_pg = PythonOperator(
        task_id='transfer_data_16_from_ms_to_pg',
        python_callable=transfer_data_16_from_ms_to_pg,
    )

    end = EmptyOperator(
        task_id='end'
    )

    start >> transfer_data_16_from_ms_to_pg >> end