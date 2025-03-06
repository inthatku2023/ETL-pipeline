# Automate ETL process with Python
import logging
import pandas as pd
import openpyxl

# setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data(file_path):
    # extract data from csv file
    
    try:
        df = pd.read_excel(file_path)
        logger.info('data extraction successful')
        return df
    except Exception as e:
        logger.error(f'error extracting data : {e}')
        raise

def transform_data(df):
    # transform data
    try:
        # remove null values
        df= df.dropna() 
        # remove blank space in column
        df.columns = df.columns.str.replace(' ','')
        logger.info("Data transformation is successful.")
        return df
    except Exception as e:
        logger.error(f"Error transform data : {e}")
        raise

def load_data(df,output_path):
    try:
        df.to_excel(output_path, index=False)
        logger.info("Data loading is successful.")
        return df
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise

def etl():
    try:
        input_file_path = "lkpAccount.xlsx"
        output_file_path = "transform_lkpAccount.xlsx"
        data = extract_data(input_file_path)
        data = transform_data(data)
        
        load_data(data,output_file_path)
        
        logger.info("ETL process  successfully.")
    except Exception as e:
        logger.error(f"error ETL process failed: {e}")
        
if __name__ == '__main__':
    etl()