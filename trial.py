# 用来测试等

import pandas as pd
time_flag_data = './data/times_csv/votes_2020-11-05 06:00:00.csv'
df = pd.read_csv(time_flag_data)
states = df['state_name'].unique()
print(len(states))
print(states)
