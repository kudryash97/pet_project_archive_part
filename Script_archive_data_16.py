import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import date

engine = create_engine('mssql+pymssql://sa:manager@DESKTOP-77162VC/electro_abramovo?charset=utf8')
today = date.today()
sub_sys_value = 16
pattern = '%EN6%'

sql_query = '''
SELECT *
FROM electro_abramovo.dbo.Link_signals
WHERE kks_id_signal LIKE %(pattern)s 
  AND kks_id_param IS NOT NULL  
  AND sub_sys = %(sub_sys_value)s;
'''
pd.set_option('display.max_columns', None)
df = pd.read_sql(sql=sql_query, con=engine, params={'pattern' : pattern, 'sub_sys_value' : sub_sys_value})

hours = pd.date_range(start=f"{today} 00:00:00", end=f"{today} 23:00:00", freq='h')
random_seconds = np.random.randint(0, 3600, 24)

df_time = pd.DataFrame({
    'time_page': hours,
    'time' : hours + pd.to_timedelta(random_seconds, unit='s'),
    'Mcs' : 476000, 'data' : 2, 'Pr' : 2, 'bstate' : 1, 'bsrc' : 0}
)
df_sql = df[['signal_indx', 'kks_id_signal']].rename(columns={'signal_indx' : 'num_sign'})
df_time['time_page'] = df_time['time_page'].astype(int) // 10**9
df_time['time'] = df_time['time'].astype(int) // 10**9
result = df_time.merge(df_sql, how='cross')
cols_order = ['time_page', 'time', 'Mcs', 'num_sign', 'data', 'Pr', 'bstate', 'bsrc', 'kks_id_signal']

df_tmp_16_state = result[cols_order]
df_tmp_16_time = df_time['time_page']
df_tmp_16_event = df_tmp_16_state[['time', 'Mcs', 'num_sign', 'data', 'Pr', 'bstate', 'bsrc', 'kks_id_signal']]
# print(df_tmp_16_state)

seconds = pd.date_range(start=f"{today} 00:00:00", end=f"{today} 23:00:00", freq='s')
df_analog = pd.read_sql(sql=sql_query, con=engine, params={'pattern' : pattern, 'sub_sys_value' : 10})
