#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pyodbc
import numpy as np
import pandas as pd
import urllib
from sqlalchemy import create_engine
import msvcrt as m
# In[2]:


data = pd.read_csv('D:\\Users\\Testing\\Desktop\\TAW\\thread_805_806\\procedure_icd_conflict_data.csv',header=None)
data.columns = ['PORTAL_TRANS_ID','ICD_CODE','SDL_CODE']
data.drop('PORTAL_TRANS_ID',axis=1,inplace=True)
data['ICD_CODE'] = data['ICD_CODE'].apply(lambda x : str(x))
data['SDL_CODE'] = data['SDL_CODE'].apply(lambda x :str(x))
data['ICD-SDL'] = data['ICD_CODE']+'-'+data['SDL_CODE']
icd_sdl_percent = data['ICD-SDL'].value_counts(normalize  = True).to_dict()
data['PERCENT'] = data['ICD-SDL'].apply(lambda x : icd_sdl_percent[x])
icd_sdl_counts = data['ICD-SDL'].value_counts().to_dict()
data['COUNTS'] = data['ICD-SDL'].apply(lambda x : icd_sdl_counts[x])
distinct_data = data.drop_duplicates()
shit = distinct_data.groupby(['ICD_CODE','SDL_CODE'])['COUNTS'].sum()/distinct_data.groupby(['ICD_CODE'])['COUNTS'].sum()
test=pd.DataFrame(shit)
test.reset_index(inplace=True)
final = test[test.COUNTS >0.001].groupby('ICD_CODE')['SDL_CODE'].apply(' '.join).reset_index()
final.SDL_CODE = final.SDL_CODE.apply(lambda x :list(map(int, x.split())))


# In[3]:


icd_sdl_dict = dict(zip(final.ICD_CODE,final.SDL_CODE))


# In[4]:


conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=192.168.1.100;'
                      'Database=FD_Stage;'
                      'UID=eman.youssef;'
                      'PWD=3adda;')


# In[5]:


sql = '''SELECT  distinct
  tb1.ICD_CODE ,
  icds.ASCII_DESC,
  tb1.sdl_code,
  tb1.RECEIVED_DESC as SDL_DESC,
  tb2.PORTAL_TRANS_ID,
  tb2.PORTAL_DATE,
  tb2.PROVIDER_ID,
  tb2.MEMBER_CODE,tb2.MEMBER_AGE,
  tb2.POLICY_NO,
  tb2.CLAIM_DATE,
  tb2.CLM_NET_AMT
  FROM [FD_Stage].[dbo].[tbl_claim_services] tb1
  join [dbo].[tbl_Claims_Submissions] tb2
  on tb1.PORTAL_TRANS_ID = tb2.PORTAL_TRANS_ID
  join [dbo].[tbl_Icd10s] icds
  on icds.ICD_CODE = tb1.ICD_CODE
  where tb1.sdl_code is not null and tb1.ICD_CODE is not null and tb1.SERVICE_TYPE ='P' '''


# In[6]:


data = pd.read_sql(sql,conn)


# In[7]:


def get_conflicts(icds,sdls,icd_sdl):
    l = []
    for i in range(0,len(icds)):
        if icds[i] in icd_sdl.keys():
            if sdls[i] in icd_sdl[icds[i]]:
                l.append(1)
            else:
                l.append(0)
        else:
            l.append(0)
    return l


# In[8]:


conflicts = get_conflicts(list(data.ICD_CODE.values),list(data.sdl_code.values),icd_sdl_dict)


# In[9]:



# In[10]:





# In[11]:


data['labels'] = pd.Series(conflicts)


# In[13]:




# In[14]:


params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=192.168.1.100;DATABASE=FD_STAGE;UID=eman.youssef;PWD=3adda")
engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
data.to_sql('thread_805',con=engine,index=False, if_exists='replace')

# In[15]:





# In[ ]:




