# 处理数据和关键事件挖掘的函数

import pandas as pd
from datetime import timedelta

def map_candidate1_votes(leading_candidates, leading_vote, trailing_vote):
    # Trump's votes
    if leading_candidates == "Trump":
        return leading_vote
    else:
        return trailing_vote

def map_candidate2_votes(leading_candidates, leading_vote, trailing_vote):
    # Biden's votes
    if leading_candidates == "Biden":
        return leading_vote
    else:
        return trailing_vote

def gen_flag_times(timeinterval1, timeinterval2, timeinterval3): #默认生成从11.04 06:00 - 11.10 24:00的所有datetime str
    start_time = pd.Timestamp(2020, 11, 4, 12, 0, 0)
    intrim_time1 = pd.Timestamp(2020, 11, 5, 12, 0, 0)
    intrim_time2 = pd.Timestamp(2020, 11, 8, 0, 0, 0)
    end_time = pd.Timestamp(2020, 11, 21, 0, 0, 0) #每6小时一个，共68个点
    c_time = start_time
    time_list = []
    while intrim_time1 > c_time:
        time_list.append(c_time)
        c_time = c_time + timedelta(hours = timeinterval1)
    while intrim_time2 > c_time:
        time_list.append(c_time)
        c_time = c_time + timedelta(hours=timeinterval2)
    while end_time > c_time:
        time_list.append(c_time)
        c_time = c_time + timedelta(hours=timeinterval3)
    flag_list = [1 for i in range(len(time_list))]
    frame_df = {'time_stamp_flag': time_list, 'flag': flag_list}
    frame_df = pd.DataFrame(frame_df)
    return time_list, frame_df

if __name__ == "__main__":
    timeinterval1 = 2
    timeinterval2 = 6
    timeinterval3 = 24
    time_list, frame_df = gen_flag_times(timeinterval1, timeinterval2, timeinterval3)
    print(time_list)
