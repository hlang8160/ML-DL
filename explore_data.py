#-*- coding: utf-8 -*-

import pandas as pd
import numpy as np

ACTION_201602_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201602.csv"
ACTION_201603_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201603.csv"
ACTION_201604_FILE = "E:/Study/pystudy/JData-master/data/JData_Action_201604.csv"
COMMENT_FILE = "E:/Study/pystudy/JData-master/data/JData_Comment.csv"
PRODUCT_FILE = "E:/Study/pystudy/JData-master/data/JData_Product.csv"
USER_FILE = "E:/Study/pystudy/JData-master/data/JData_User.csv"
NEW_USER_FILE = "E:/Study/pystudy/JData-master/data/JData_User_New.csv"

# Display format
pd.options.display.float_format = '{:,.3f}'.format           #对数据处理，只保留三维小数点，


def convert_age(age_str):             #首先对年龄进行处理
    if age_str == u'-1':
        return -1
    elif age_str == u'15岁以下':
        return 0
    elif age_str == u'16-25岁':
        return 1
    elif age_str == u'26-35岁':
        return 2
    elif age_str == u'36-45岁':
        return 3
    elif age_str == u'46-55岁':
        return 4
    elif age_str == u'56岁以上':
        return 5
    else:
        return -1

def tranform_user_age():                                  #对年龄的转换处理
    # Load data, header=0 means that the file has column names
    df = pd.read_csv(USER_FILE, header=0, encoding="gbk")     #采用gbk编码方式，能够读取中文
    #df=pd.read_csv(USER_FILE,header=0,encoding="gbk")
    # for i in range(len(df['age'])):
    #     print(i)
    #     if df['age'][i] == u"15岁以下":
    #         df['age'][i] = 0
    #     elif df['age'][i] == u"16-25岁":
    #         df['age'][i] = 1
    #     elif df['age'][i] == u"26-35岁":
    #         df['age'][i] = 2
    #     elif df['age'][i] == u"36-45岁":
    #         df['age'][i] = 3
    #     elif df['age'][i] == u"46-55岁":
    #         df['age'][i] = 4
    #     elif df['age'][i] == u"56岁以上":
    #         df['age'][i] = 5
    #     else:
    #         df['age'][i] = -1

    df['age'] = df['age'].map(convert_age)   #将每个df['age].map(convert_age)
    df['user_reg_tm'] = pd.to_datetime(df['user_reg_tm'])    #将注册时间改为pandas标准时间格式pd.to_datetime(df[''])
    min_date = min(df['user_reg_tm'])         #得出注册时间最小值

    df['user_reg_diff'] = [i for i in (df['user_reg_tm'] - min_date).dt.days]

    df.to_csv(NEW_USER_FILE, index=False)



def explore_user():
    df = pd.read_csv(NEW_USER_FILE, header=0)
    # Get first 5 rows, also you can use df.tail(10) to get last 10 rows
    print(df.head(5))
    # Basic statistical information
    print(df.describe())
    # Each column type
    print(df.dtypes)


def explore_action_02(chunk_size=10000):
    # Number of Record: 18117303
    reader = pd.read_csv(ACTION_201602_FILE, header=0, iterator=True)#允许迭代，按块读取
    chunks = []
    loop = True
    while loop:
        try:
            chunk = reader.get_chunk(chunk_size)       #每次获取到1000行数据，一次读取全部数据太大，分块读取，然后再上下连接
            chunks.append(chunk)
            print("Iteration update")
        except StopIteration:
            loop = False
            print("Iteration is stopped")

    df = pd.concat(chunks, ignore_index=True)      #连接轴上的索引，产生一组新的索引  range(total_length)   两个索引并集
    print(df.head(5))
    print(df.dtypes)

    print(df[df["user_id"] == 279749])

if __name__ == "__main__":
    # 进行年龄映射
   # tranform_user_age()

    #explore_user()
    explore_action_02()
