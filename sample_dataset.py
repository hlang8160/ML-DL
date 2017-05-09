#-*- coding: utf-8 -*-

ACTION_201602_FILE = "JData_Action_201602.csv"
ACTION_201603_FILE = "JData_Action_201603.csv"
ACTION_201604_FILE = "JData_Action_201604.csv"
USER_FILE = "JData_User.csv"
COMMENT_FILE = "JData_Comment.csv"
PRODUCT_FILE="JData_Product.csv"

file_list = [ACTION_201602_FILE, ACTION_201603_FILE,ACTION_201604_FILE,
             USER_FILE, COMMENT_FILE, PRODUCT_FILE]

for fname in file_list:
    with open("E:/Study/pystudy/JData-master/JData/" + fname, 'rb') as fi:        #都采用  with open("E:/"+fname，'rb') as fi:
        with open('E:/Study/pystudy/JData-master/data/' + fname, 'wb') as fo:     #with open('E:/'+fname, 'wb') as fo:   读取或者写入完之后会自动调用close
            for i in range(30000):
                fo.write(fi.readline())   #fi.readline()读取一行，然后在写入，fo.wirte()
