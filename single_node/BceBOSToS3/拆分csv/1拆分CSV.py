#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: gxs
@license: (C) Copyright 2016-2019, Light2Cloud (Beijing) Web Service Co., LTD
@contact: dingjianfeng@light2cloud.com
@software: AWS-DJF
@file: 1拆分CSV.py
@ide: PyCharm
@time: 2020/5/6 14:10
@desc：
"""
import os
import pandas as pd


# filename为文件路径，file_num为拆分后的文件行数
# 根据是否有表头执行不同程序，默认有表头的

def Data_split(filename, file_num, header=True):
    if header:
        # 设置每个文件需要有的行数,初始化为 1000W
        chunksize = 10000
        data1 = pd.read_table(filename, chunksize=chunksize, sep=',', encoding='gbk')
        print("data1:", data1)
        # num 表示总行数
        num = 0
        for chunk in data1:
            num += len(chunk)
        print("num:", num)
        # chunksize表示每个文件需要分配到的行数
        chunksize = round(num / file_num + 1)
        print("chunksize:", chunksize)

        head, tail = os.path.split(filename)
        print(os.path.abspath(filename))
        data2 = pd.read_table(filename, chunksize=chunksize, sep=',', encoding='gbk')
        i = 1
        for chunk in data2:
            chunk.to_csv('{0}_{1}'.format(i, tail), header=None, index=False)
            print(f'带有表头的文件 {filename} ：保存第 {i} 个数据')
            i += 1
    else:
        # 获得每个文件需要的行数
        chunksize = 10000
        data1 = pd.read_table(filename, chunksize=chunksize, header=None, sep=',')
        num = 0
        for chunk in data1:
            num += len(chunk)
            chunksize = round(num / file_num + 1)

            head, tail = os.path.split(filename)
            data2 = pd.read_table(filename, chunksize=chunksize, header=None, sep=',')
            i = 1
            for chunk in data2:
                chunk.to_csv('{0}_{1}'.format(i, tail), header=None, index=False)
                print('没有表头的文件：保存第{0}个数据'.format(i))
                i += 1


# filename 为存放 csv 文件的路径
# file_num 为拆分为的文件个数, 默认10份
Data_split('', 10, header=True)
