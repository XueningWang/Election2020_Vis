# 该脚本用于将按小时拆分的选票结果数据处理成用于地图可视化的json文件

import pandas as pd
import json
import os

def data2col_dict(data_file): #先将信息构建成json（dict）形式
    data_df = pd.read_csv(data_file)
    state_dict = {}
    for i in range(len(data_df)): #对每个时间点处
        state_df = data_df.iloc[i, :]
        state_name = state_df['state_name']
        state_data = {"name": state_name, "Biden_votes": state_df['Biden_votes'], "Trump_votes": state_df['Trump_votes'],
                      "leading_candidate_name": state_df['leading_candidate_name'], "total_votes_count": state_df["total_votes_count"]}
        state_dict[state_name] = state_data
    return state_dict

def data2json(sample_json_file, state_dict):
    f = open(sample_json_file, 'r')
    sample_json = f.readlines()[0]
    sample_dict = json.loads(sample_json)
    res_geo = sample_dict["objects"]["states"]["geometries"]
    for i in range(len(res_geo)):
        req_state_name = res_geo[i]["properties"]["name"]
        try:
            state_data = state_dict[req_state_name]
        except Exception as e:
            print(e)
            continue
        res_geo[i]["properties"] = state_data
    return sample_dict #预期是浅复制，可以直接用

if __name__ == "__main__":
    sample_json_file = "../usa_sample.json"
    data_dir = "../data/times_csv/"
    save_dir = "../data/map_json/"
    for f in os.listdir(data_dir):
        time_name = f[6:-4]
        f_path = data_dir + f
        state_dict = data2col_dict(f_path)
        res_dict = data2json(sample_json_file, state_dict)
        # 存储
        save_path = save_dir + time_name + '.json'
        save_dict = json.dumps(res_dict)
        save_f = open(save_path, 'w')
        save_f.write(save_dict)



