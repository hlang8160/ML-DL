#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np
from collections import Counter

ACTION_201602_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201602.csv"
ACTION_201603_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201603.csv"
ACTION_201604_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201604.csv"
COMMENT_FILE = "E:/Study/pystudy/JData-master/data/JData_Comment.csv"
PRODUCT_FILE = "E:/Study/pystudy/JData-master/data/JData_Product.csv"
USER_FILE = "E:/Study/pystudy/JData-master/data/JData_User.csv"
NEW_USER_FILE = "E:/Study/pystudy/JData-master/data/JData_User_New.csv"
USER_TABLE_FILE = "E:/Study/pystudy/JData-master/data/user_table.csv"

pd.options.display.float_format = '{:,.3f}'.format

def get_from_jdata_user():
    # 从做完年龄映射的NEW_USER_FILE中读取,
    # 这步之前需要先运行explore_data.py中
    # 的tranform_user_age函数
    df_usr = pd.read_csv(NEW_USER_FILE, header=0)
    df_usr = df_usr[["user_id", "age", "sex", "user_lv_cd"]]     #剔除用户注册时间和注册时间差
    return df_usr


# apply type count
def add_type_count(group):                      #输入的是一个dataframe,将
    behavior_type = group.type.astype(int)     #输入的是action，将ACTION中的type转换成原本为int64转为int32 group['type'].astype(int)
                                                #返回的是一个int类型改列所有值
    type_cnt = Counter(behavior_type)       #是一个dict类型，key-value一一对应      from collections import counter Counter类是一个dict
                 #行为数据1.浏览2.加入购物车3.购物车删除4.下单5.关注6.点击
    group['browse_num'] = type_cnt[1]       #添加新的一
    group['addcart_num'] = type_cnt[2]
    group['delcart_num'] = type_cnt[3]
    group['buy_num'] = type_cnt[4]
    group['favor_num'] = type_cnt[5]
    group['click_num'] = type_cnt[6]    #返回新的用户action只包含用户id和浏览数，加购数

    return group[['user_id', 'browse_num', 'addcart_num',
                  'delcart_num', 'buy_num', 'favor_num',
                  'click_num']]


def get_from_action_data(fname, chunk_size=10000):  #按块读取，
   #  chunks=[]
   #  reader=pd.read_csv(fname,chunksize=chunk_size)
   # # reader1=reader[["user_id","type"]]
   #  for chunk in reader:
   #      chunks.append(chunk)
   #      print("Success")


    chunks = []
    reader = pd.read_csv(fname, header=0, iterator=True)     #指定iterator=True  后面可以使用get_chunk(chunk_size)获取
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)[["user_id", "type"]]     #只读取用户id和type
            chunks.append(chunk)
            print("Success")
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df_ac = pd.concat(chunks, ignore_index=True)         #ignore_index=True，不保留连接轴上的索引，产生一组新的索引，
    # 默认是行连接，范围range(total_length)  列coluumns是并集。    索引这样是行连接，将不同chunks上下行连接，产生一组新的行索引

    df_ac = df_ac.groupby(['user_id'], as_index=False).apply(add_type_count)    #默认as_index=True都是以聚合的形式，False，以无索引，非层次化的
    # Select unique row
    df_ac = df_ac.drop_duplicates('user_id')    #去除重复的用户id，我只要改用户的id和该用户的浏览数，购买数等。

    return df_ac


def merge_action_data():
    df_ac = []
    df_ac.append(get_from_action_data(fname=ACTION_201602_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201603_FILE))
    df_ac.append(get_from_action_data(fname=ACTION_201604_FILE))

    df_ac = pd.concat(df_ac, ignore_index=True)
    df_ac = df_ac.groupby(['user_id'], as_index=False).sum()     #将相同用户id的浏览数、加购数、点击数、购买数

    df_ac['buy_addcart_ratio'] = df_ac['buy_num'] / df_ac['addcart_num']
    df_ac['buy_browse_ratio'] = df_ac['buy_num'] / df_ac['browse_num']
    df_ac['buy_click_ratio'] = df_ac['buy_num'] / df_ac['click_num']
    df_ac['buy_favor_ratio'] = df_ac['buy_num'] / df_ac['favor_num']

    df_ac.ix[df_ac['buy_addcart_ratio'] > 1, 'buy_addcart_ratio'] = 1    #df_ac.ix[ , ]=1选取行和列，并且赋值
    df_ac.ix[df_ac['buy_browse_ratio'] > 1, 'buy_browse_ratio'] = 1
    df_ac.ix[df_ac['buy_click_ratio'] > 1, 'buy_click_ratio'] = 1
    df_ac.ix[df_ac['buy_favor_ratio'] > 1, 'buy_favor_ratio'] = 1

    return df_ac


if __name__ == "__main__":

    user_base = get_from_jdata_user()
    user_behavior = merge_action_data()
    #user_behavior=get_from_action_data(fname=ACTION_201602_FILE)
    # SQL: left join
    user_behavior = pd.merge(user_base, user_behavior, on=['user_id'], how='left')     #以

    user_behavior.to_csv(USER_TABLE_FILE, index=False)
