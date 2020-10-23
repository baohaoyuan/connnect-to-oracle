#!/usr/bin/env python
# -*- coding: UTF-8 -*-
import os
import time
import sys  
import datetime
import subprocess
import cx_Oracle
import numpy as np
import pandas as pd 
global inp_date
try:
    inp_date = datetime.date(*tuple(map(int, sys.argv[1].split('-'))))
except:
    inp_date = None
LMS = {   
        "host": "",
        "port": ,
        "user": "",
        "psw": "",
        "service": "",
    }
DWH = {   
        "host": "",
        "port": ,
        "user": "",
        "psw": "",
        "service": "",
    }
def make_query(query_path):
    # read query
    with open(query_path, 'r') as f:
        query = f.read()
    if "between '{}' and '{}'" in query:
        if inp_date is None:
            arg1 = datetime.datetime.strftime(datetime.date.today(), '%d-%b-%y')
            arg2 = datetime.datetime.strftime(datetime.date.today() + datetime.timedelta(days=1), '%d-%b-%y')
        else:
            arg1 = datetime.datetime.strftime(inp_date, '%d-%b-%y')
            arg2 = datetime.datetime.strftime(inp_date + datetime.timedelta(days=1), '%d-%b-%y')
        query = query.format(arg1, arg2)
    else:
        if inp_date is None:
            arg = datetime.datetime.strftime(datetime.date.today(), '%d-%b-%y')
        else:
            arg = datetime.datetime.strftime(inp_date, '%d-%b-%y')
        query = query.format(arg)
    return query
def connect_and_query(conn_str, query_str):
    connection_str = '{user}/{psw}@{host}:{port}/{service}'.format(**conn_str)
    conn = cx_Oracle.connect(connection_str)
    # set large array size so that number of read/writes to DB are minimized
    cur = conn.cursor()
    cur.arraysize = 1000
    time.sleep(1)
    try:
        print("Executing query...")
        print(query_str)
        data = cur.execute(query_str).fetchall()
        col_names = [row[0] for row in cur.description]
        df = pd.DataFrame(data, columns=col_names)
        print("Fetched {} records".format(len(df)))
        return df
    except:
        print("Query failed")
def join_data(df_lms, df_dwh):
    # put more logic to determine planned and executed sequence
    df = df_lms.merge(df_dwh, on=['ORDER_ID'], how='left')
    print(df.head())
    print(df.columns)
    print("Merged {} rows".format(len(df)))
    # also if there is any sorting needed    
    df.sort_values(by=['STORE_ID', 'VAN_ID','TRIP_NUMBER', 'ACTUAL_ARRIVAL_TIME'], inplace=True)
    return df
if __name__ == "__main__":
    if sys.argv[1] is None:
        DATE = datetime.datetime.strftime(datetime.date.today(), '%d-%b-%y')
    else:
        DATE  = datetime.datetime.strftime(inp_date, '%d-%b-%y')
    print("Fetching data for {}".format(DATE))
    q_lms_raw = make_query('lms_raw.sql')
    q_dwh_raw = make_query('dwh_raw.sql')
    if not os.path.exists('dst_lms_{}.csv'.format(DATE)):
        df_lms = connect_and_query(LMS, q_lms_raw)
        df_lms.to_csv('dst_lms_{}.csv'.format(DATE), index=False)
    else:
        df_lms = pd.read_csv('dst_lms_{}.csv'.format(DATE))
    if not os.path.exists('dst_dwh_{}.csv'.format(DATE)):
        df_dwh = connect_and_query(DWH, q_dwh_raw)
        df_dwh.to_csv('dst_dwh_{}.csv'.format(DATE), index=False)
    else:
        df_dwh = pd.read_csv('dst_dwh_{}.csv'.format(DATE))
    df_all = join_data(df_lms, df_dwh)
    df_all.to_csv('dst_pilot_{}.csv'.format(DATE), index=False)
    if not os.path.exists('dst_summary_{}.csv'.format(DATE)):
        q_lms_agg = make_query('lms_agg.sql')
        df_summary = connect_and_query(LMS, q_lms_agg)
        df_summary.to_csv('dst_summary_{}.csv'.format(DATE), index=False)