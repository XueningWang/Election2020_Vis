## 该脚本根据所有州实时更新的选举数据，计算得出每隔一段时间间隔的静态投票局势。


# 输出数据格式：每小时一个csv文件，每一行是当前时间每个州的基本信息及投票情况（画一张地图需要的所有数据）。
# 列包括：时间（datetime）、州名称、领先者、非领先者；
#        B获选票、T获选票、总开票数（total_votes_count）、剩余未开票选票；
#        总收到选票数（总开票数+剩余未开票选票）、双方各自占总开票数的比例（B or T选票/总开票数）、开票比例（总开票数/总开票+剩未开票数）

# 思路：先根据州将数据进行划分（用字典类型），去除用不到的数据
#      先遍历一次，将每段时间间隔的静态数据插入到所有数据里面，并标记FLAG
#      在每两行出现变动的时候，保留后一行的所有信息并标注FLIP，并将这个翻转信息添加到最近的下一个FLAG行里，并且按照翻转次数给编号
#      最后保留所有的FLAG信息（每个州一张表、concat后每个时段一张表）和FLIP信息（一张大表）

import pandas as pd
pd.set_option('display.max_columns', 10)
import copy

from utils import *

def load_clean_split_data(data_file):
    data = pd.read_csv(data_file)
    # change state name
    data['state_name'] = data['state'].map(lambda s: s.split(' (')[0])
    # clear useless data
    data_cleaned = data[['state_name', 'timestamp',
                         'leading_candidate_name', 'trailing_candidate_name',
                         'leading_candidate_votes', 'trailing_candidate_votes', 'votes_remaining','total_votes_count']]
    # generate useful data
    data_cleaned['Trump_votes'] = data_cleaned.apply(lambda row: map_candidate1_votes(row['leading_candidate_name'],
                                                                                      row['leading_candidate_votes'],
                                                                                      row['trailing_candidate_votes']),
                                                     axis = 1)
    data_cleaned['Biden_votes'] = data_cleaned.apply(lambda row: map_candidate2_votes(row['leading_candidate_name'],
                                                                                      row['leading_candidate_votes'],
                                                                                      row['trailing_candidate_votes']),
                                                     axis = 1)
    data_cleaned['Trump_prop'] = data_cleaned['Trump_votes'] / data_cleaned['total_votes_count']
    data_cleaned['Biden_prop'] = data_cleaned['Biden_votes'] / data_cleaned['total_votes_count']
    data_cleaned['total_votes_received'] = data_cleaned['total_votes_count'] + data_cleaned['votes_remaining']
    data_cleaned['vote_count_prop'] = data_cleaned['total_votes_count'] / data_cleaned['total_votes_received']
    # clean and transfer time
    data_cleaned['timestamp_hour'] = data_cleaned['timestamp'].map(lambda s: s.split('.')[0])
    data_cleaned.drop(columns = ['timestamp'], inplace = True)
    data_cleaned['time_stamp'] = data_cleaned['timestamp_hour'].map(lambda s: pd.Timestamp(s))
    data_cleaned.drop(columns = ['timestamp_hour'], inplace = True)
    # split as states_csv
    states = data_cleaned['state_name'].unique()
    print("All States: \n", states)
    state_df = {}
    for s in states:
        state_df[s] = data_cleaned.loc[data_cleaned['state_name'] == s]
    #print(state_df['Florida'])
    return states, state_df

def set_time_flag(df, state_name, timeinterval1, timeinterval2, timeinterval3):
    _, frame_df = gen_flag_times(timeinterval1, timeinterval2, timeinterval3)
    df = pd.concat([df, frame_df], axis=0, join='outer')
    # 将time flag插入
    df = df.reset_index(drop=True)
    len = df.shape[0] #行数
    for i in range(len):
        if df.at[i, 'flag'] == 1:
            df.at[i, 'time_stamp'] = df.at[i, 'time_stamp_flag']
    df.sort_values(by = ['time_stamp'], ascending=True, inplace=True)
    # 整理time flag的数据
    df = df.reset_index(drop=True)
    first_flag = 0
    while df.at[first_flag, 'flag'] == 1: #首先找到第一个有数据的点
        first_flag += 1
    for i in range(len):
        if i < first_flag:
            df.at[i, 'state_name'] = state_name # 除州名外，其他数据保持NaN
        elif df.at[i, 'flag'] == 1:
            # 很笨的乾坤大挪移
            df.at[i, 'state_name'] = df.at[i - 1, 'state_name']
            df.at[i, 'leading_candidate_name'] = df.at[i - 1, 'leading_candidate_name']
            df.at[i, 'trailing_candidate_name'] = df.at[i - 1, 'trailing_candidate_name']
            df.at[i, 'leading_candidate_votes'] = df.at[i - 1, 'leading_candidate_votes']
            df.at[i, 'trailing_candidate_votes'] = df.at[i - 1, 'trailing_candidate_votes']
            df.at[i, 'votes_remaining'] = df.at[i - 1, 'votes_remaining']
            df.at[i, 'total_votes_count'] = df.at[i - 1, 'total_votes_count']
            df.at[i, 'Trump_votes'] = df.at[i - 1, 'Trump_votes']
            df.at[i, 'Biden_votes'] = df.at[i - 1, 'Biden_votes']
            df.at[i, 'Trump_prop'] = df.at[i - 1, 'Trump_prop']
            df.at[i, 'Biden_prop'] = df.at[i - 1, 'Biden_prop']
            df.at[i, 'total_votes_received'] = df.at[i - 1, 'total_votes_received']
            df.at[i, 'vote_count_prop'] = df.at[i - 1, 'vote_count_prop']
        else:
            continue
    df.drop(columns=['time_stamp_flag'], inplace=True)
    df['flip'] = 'no'

    return df, first_flag #带有flag和原始数据

def find_flip(df, first_flag):
    len = df.shape[0]
    last_real_index = first_flag
    last_flip_real_index = -1
    for i in range(len):
        if df.at[i, 'flag'] == 1: #记录上一个翻的信息（如果有）
            if last_flip_real_index >=0:
                df.at[i, 'flip'] = df.at[last_flip_real_index, 'flip']
                last_flip_real_index = -1 #只有最近的一个记录
        else:
            if df.at[last_real_index, 'leading_candidate_name'] != df.at[i, 'leading_candidate_name']: #翻了
                last_flip_real_index = i
                if df.at[i, 'leading_candidate_name'] == 'Trump':
                    df.at[i, 'flip'] = 'red'
                elif df.at[i, 'leading_candidate_name'] == 'Biden':
                    df.at[i, 'flip'] = 'blue'
            last_real_index = i
    return df

def save_flag(df, state_name):
    df = df[df['flag'] == 1]
    save_file = '../data/states_csv/votes_flag_%s_till11-20.csv'%state_name
    df.to_csv(save_file, index = None)

def save_flip(states, state_df):
    res_state_df = copy.deepcopy(state_df)
    for s in states:
        s_df = state_df[s]
        s_df = s_df[s_df['flip'] != 'no']
        state_df[s] = s_df[s_df['flag'] != 1]
    first_state = states[0]
    res_df = state_df[first_state]
    for si in range(1, len(states)):
        res_df = pd.concat([res_df, state_df[states[si]]], axis=0)
    res_df.to_csv("../data/votes_flips_all.csv", index = None)
    return res_state_df

def save_flag_by_time(states, state_df):
    for s in states:
        s_df = state_df[s]
        state_df[s] = s_df[s_df['flag'] == 1]
    # 先合并成大表，按照时间排序
    first_state = states[0]
    all_flag_df = state_df[first_state]
    for si in range(1, len(states)):
        all_flag_df = pd.concat([all_flag_df, state_df[states[si]]], axis=0)
    all_flag_df.sort_values(by = ['time_stamp'], ascending=True, inplace=True)
    all_flag_df = all_flag_df.reset_index(drop=True)
    # 按时间分割存储
    c_time = all_flag_df.at[0, 'time_stamp']
    last_row = 0
    for i in range(all_flag_df.shape[0]):
        if all_flag_df.at[i, 'time_stamp'] == c_time:
            continue
        else:
            all_flag_df.loc[last_row: i-1].to_csv("../data/times_csv/votes_%s.csv"%c_time)
            last_row = i
            c_time = all_flag_df.at[i, 'time_stamp']

if __name__ == "__main__":
    all_data_file = "../data/all-state-changes.csv"
    states, state_df = load_clean_split_data(all_data_file)
    timeinterval1, timeinterval2, timeinterval3 = 2, 6, 24

    # 对每个州处理并保存数据
    for s in states:
        state_df[s], first_flag = set_time_flag(state_df[s], s, timeinterval1, timeinterval2, timeinterval3)
        state_df[s] = find_flip(state_df[s], first_flag)
        save_flag(state_df[s], s)
        # print("Process done with %s"%s)

    backup_state_df = save_flip(states, state_df)
    save_flag_by_time(states, backup_state_df)
