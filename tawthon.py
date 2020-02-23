import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from sklearn.metrics import confusion_matrix ,classification_report
import pyodbc
import numpy as np
import pandas as pd
import urllib
from sqlalchemy import create_engine
from sklearn.preprocessing import StandardScaler, LabelEncoder
import lightgbm as lgb
import msvcrt as m
from sklearn.model_selection import train_test_split

#get data from the database giving the connection string and the sql query
def get_data():
    sql = '''select distinct clm.PAID_AMT,
	   serv.ICD_CODE,
	   serv.sdl_code,
	   clm.TOT_CLM_GROSS_AMT,
	   clm.TOT_CLM_NET_AMT,
	   clm.PROVIDER_ID,
	   clm.AGE,
       clm.PORTAL_TRANS_ID,
	   clm.TOT_NON_PAY_AMT,
	   clm.TOT_PROCESSED_GROSS_AMT,
	   clm.BENEFIT_CAT,
	   clm.GENDER,
	   clm.CLAIM_TYPE
        from tbl_Claims_Submissions clm 
        join tbl_claim_services serv
        on clm.PORTAL_TRANS_ID = serv.PORTAL_TRANS_ID
        where PORTAL_DATE >  CONVERT(Datetime,'2018-10-18') - 1'''
    conn = pyodbc.connect('Driver={SQL Server};'
                      'Server=192.168.1.100;'
                      'Database=FD_Stage;'
                      'UID=eman.youssef;'
                      'PWD=3adda;')
    data = pd.read_sql(sql,conn)
    return data

    #get the portal trans ID 
def get_po_id(data):
    return data.PORTAL_TRANS_ID
#change these columns data types from float to categorical
def change_data_type(data):
    data.BENEFIT_CAT = data.BENEFIT_CAT.astype('O')
    data.sdl_code = data.sdl_code.astype('O')
    return data

#fill the null values of the continous data columns with its mean
def impute_float_cols(data):
    for i in data.columns:
        if data[i].dtype == 'float64':
            data[i] = data[i].fillna(data[i].mean())
    return data

#apply standarization for the continous data
def standarize_continous_data(data):
    for i in data.columns:
        if data[i].dtype == 'float64':
            scaler = StandardScaler()
            data[i] = scaler.fit_transform(data[i].values.reshape(-1,1))
    return data

#assure that every categorical data has data type string 
def assure_strings(data):
    for i in data.columns:
        if data[i].dtype == 'O':
            data[i] = data[i].apply(lambda x:str(x))
    return data

#impute the nulls with a category string null as it may contains a meaning
def impute_catgorical_nulls(data):
    for i in data.columns:
        if data[i].dtype == 'O':
            data[i].fillna('null',inplace=True)
    return data

#label encoding for categorical features
def label_encode_categories(data):
    for i in data.columns:
        if data[i].dtype == 'O':
            LB = LabelEncoder()
            data[i] = LB.fit_transform(data[i].values.reshape(-1,1))
    return data

#defining a preprocessing function 
def preprocessing(data):
    data = change_data_type(data)
    data = impute_float_cols(data)
    data = standarize_continous_data(data)
    data = assure_strings(data)
    data = impute_catgorical_nulls(data)
    data = label_encode_categories(data)
    return data

#load The saved model and predict the output
def predict(X):
    bst = lgb.Booster(model_file='fraud_classefier.txt')
    Y = bst.predict(X)
    Y_classes = pd.Series(Y).apply(lambda x: 1 if x >=0.1 else 0)
    return Y_classes

def concat_output(Y_classes,PORTAL_TRANS_ID):
    output =  pd.concat([PORTAL_TRANS_ID,Y_classes],axis=1)
    output.columns = ['PORTAL_TRANS_ID','Fraud_class']
    return output


def insert_to_db(output):
    params = urllib.parse.quote_plus("DRIVER={SQL Server};SERVER=192.168.1.100;DATABASE=FD_old;UID=eman.youssef;PWD=3adda")
    engine = create_engine("mssql+pyodbc:///?odbc_connect=%s" % params)
    output.to_sql('MLFDS_V1',con=engine,index=False, if_exists='replace')

data = get_data()
PORTAL_TRANS_ID = get_po_id(data)
X = preprocessing(data)
Y = predict(X)
output = concat_output(Y,PORTAL_TRANS_ID)
insert_to_db(output)
