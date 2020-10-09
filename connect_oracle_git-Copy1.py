#!/usr/bin/env python
# coding: utf-8

# '''
# import cx_Oracle
# dsn_tns = cx_Oracle.makedsn('server', 'port', service_name='service_name')
# #dsn_tns = cx_Oracle.makedsn('server', 'port', 'sid')
# conn = cx_Oracle.connect(user='username', password='password', dsn=dsn_tns)
# c = conn.cursor()
# c.execute('select count(*) from TABLE_NAME')
# for row in c:
#    print(row)
# conn.close()
# '''

# In[ ]:





# In[21]:


import cx_Oracle
import pandas as pd
from collections import defaultdict
from datetime import timedelta
import operator
import numpy as np
import tqdm
CONN_INFO_wh = {
    'host': '',
    'port': ,
    'user': '',
    'psw': '',
    'service': '',
}
CONN_INFO_prod = {
    'host': '',
    'port': ,
    'user': '',
    'psw': '',
    'service': '',
}
##CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO_prod)

CONN_STR = '{user}/{psw}@{host}:{port}/{service}'.format(**CONN_INFO_wh)
conn = cx_Oracle.connect(CONN_STR)


# In[2]:





# In[22]:


cur = conn.cursor()
cur.arraysize = 100000
time.sleep(1)


# In[12]:


try:
    query = '''
         select
    
             '''
    df = pd.read_sql(con = conn, sql = query)
finally:
    conn.close()
df.head()


# In[23]:


try:
    query = '''
           select 
 

    '''
    df1 = pd.read_sql(con = conn, sql = query)
finally:
    conn.close()
df1.head()


# In[ ]:


df1


# In[19]:


conn = cx_Oracle.connect(CONN_STR)
c = conn.cursor()

c.execute('''    ''')
for result in c:
    print(result)
    


# In[ ]:


cur = con.cursor()
cur.execute('select * ')
for result in cur:
    print result


# In[ ]:





# In[ ]:


try:
    query = '''
           select  '
    '''
    df1 = pd.read_sql(con = conn, sql = query)
finally:
    conn.close()
df1.head()


# In[5]:


df1


# In[ ]:





# In[4]:


try:
    query = '''
           select  '

    
             '''
    df1 = pd.read_sql(con = conn, sql = query)
finally:
    conn.close()
df1.head()


# In[ ]:


SELECT  *
FROM    (
        SELECT  *
        FROM    mytable
        ORDER BY
                dbms_random.value
        )
WHERE rownum <= 1000


# In[ ]:


df.shape


# In[ ]:


type(df)


# In[ ]:


df.tail()


# In[ ]:


df.to_csv(file_name, encoding='utf-8', index=False)


# In[ ]:


newdf = df[(df.FIRST_NAME != "ASDA") & (df.LAST_NAME != "TEST")]


# In[ ]:


newdf.tail()


# In[ ]:


newdf.shape


# In[ ]:


s=newdf.sample(n=100)


# In[ ]:


s


# In[ ]:


import os
print(os.getcwd())


# In[ ]:


import os
print(os.getcwd())
os.chdir('/Users/vn060tw/Documents')


# In[ ]:


f1='email_cnc3.csv'


# In[ ]:


newdf.to_csv(f1, sep=',',index=False,header=True) 


# In[ ]:


newdf.shape


# In[ ]:


newdf.sort_values(['LAST_NAME', 'FIRST_NAME'], ascending=[True, True])


# In[ ]:


qy='select * 10'


# In[ ]:


c.execute(qy)


# In[ ]:


df = pd.read_sql(con = conn, sql = query)
df.head()


# In[ ]:


import cx_Oracle

dsn_tns = cx_Oracle.makedsn('', '', service_name='') 
conn = cx_Oracle.connect(user=r'', password='',dsn=dsn_tns)
c = conn.cursor()


# In[ ]:


qy='select * where ROWNUM <= 10'


# In[ ]:


c.execute(qy)


# In[ ]:


df=c.execute(qy)
df.head()


# In[ ]:


c.execute(u'select username from dba_users')


# In[ ]:





# In[ ]:





# In[ ]:


import pandas as pd
import cx_Oracle
conn= cx_Oracle.connect('')
try:
    query = '''
         select *  ROWNUM <= 10
             '''
    df = pd.read_sql(con = conn, sql = query)
finally:
    conn.close()
df.head()


# In[ ]:


c.execute('select count(*) ')

