import pandas as pd
from sqlalchemy import create_engine
from datetime import date
import random

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

engine = create_engine('mssql+pymssql://sa:manager@DESKTOP-77162VC/electro_abramovo?charset=utf8')
today = date.today()
sub_sys_value = 10
pattern = '%EN6%'

sql_query = '''
SELECT kks_id_signal, signal_indx, dimension
FROM electro_abramovo.dbo.Link_signals
WHERE kks_id_signal LIKE %(pattern)s 
  AND kks_id_param IS NOT NULL  
  AND sub_sys = %(sub_sys_value)s;
'''
pd.set_option('display.max_columns', None)
df = pd.read_sql(sql=sql_query, con=engine, params={'pattern' : pattern, 'sub_sys_value' : sub_sys_value})

seconds = pd.date_range(start=f"{today} 00:00:00", end=f"{today} 23:59:59", freq='s')

df_time = pd.DataFrame({
    'time': seconds,
    'Mcs' : 476000,
    # 'data' : df['dimension'].apply(gen_value),
    'bzone' : 0,
    'isevnt' : 1,
    'bstate' : 1,
    'bsrc' : 0}
)
df_sql = df[['signal_indx', 'kks_id_signal', 'dimension']].rename(columns={'signal_indx' : 'num_sign'})

df_time['time'] = df_time['time'].astype(int) // 10**9
result = df_time.merge(df_sql, how='cross')
result['data'] = result['dimension'].apply(gen_value)
cols_order = ['time', 'Mcs', 'num_sign', 'data', 'bzone', 'isevnt', 'bstate', 'bsrc', 'kks_id_signal']
cols_order_state = ['time_page', 'time', 'Mcs', 'num_sign', 'data', 'bzone', 'isevnt', 'bstate', 'bsrc', 'kks_id_signal']

df_tmp_10_event = result[cols_order]
df_tmp_10_state = df_tmp_10_event.iloc[::5].copy()
df_tmp_10_state['time_page'] = df_tmp_10_state['time']
df_tmp_10_state = df_tmp_10_state[cols_order_state]
df_tmp_10_time = df_tmp_10_event['time'].iloc[::5].copy()
# print(df_tmp_10_state)



