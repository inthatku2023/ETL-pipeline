#import library
from sqlalchemy import create_engine
import pyodbc
import logging
import pandas as pd
import openpyxl
import os

# get password from environment var
pwd = '1234'
uid = 'postgres'
server = "localhost"
db = "menu"
port = "5432"
dir = r'D:\PostgreSQL\etl toturial\etl to pgsql'

# extract data from sql server
def extract():
    try:
        directory = dir
        for filename in os.listdir(directory):
            # get filename without extension
            file_wo_ext = os.path.splitext(filename)[0]
            # only process excel files
            if filename.endswith(".xlsx"):
                f = os.path.join(directory,filename)
                # checking if it is a file
                if os.path.isfile(f):
                    df = pd.read_excel(f)
                    #call to load
                    load(df,file_wo_ext)
    except Exception as e:
        print("Data extract error: "+ str(e))

#load data to postgres
def load(df,tbl):
    try:
        row_imported = 0
        engine = create_engine(f'postgresql://{uid}:{pwd}@{server}:{port}/{db}')
        print(f"importing rows {row_imported} to {row_imported + len(df)}...")
        # save df to postgres
        df.to_sql(f"stg_{tbl}",engine, if_exists = 'replace', index = False)
        row_imported += len(df)
        # add notifications
        print("data load success :" + f"stg_{tbl}")
    except Exception as e:
        print("Data load error: "+ str(e))
        
try:
    df = extract()
    
except Exception as e:
     print("Error while extracting data: "+ str(e))