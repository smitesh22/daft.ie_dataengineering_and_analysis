import os
import re
import pandas as pd
from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.snowflake.operators.snowflake import  SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup


from astro import sql as aql

S3_FILE_PATH = "s3://daftsnowflake/data_v1.csv"

default_args = {
    'owner': 'smitesh22',
    'email': ['smitesh22@gmail.com']
}



with DAG(dag_id="daft_pipeline", start_date=datetime(2023, 9, 14), schedule='@daily', catchup=False) as dag:

    
    
    with TaskGroup("Extract") as extract:
        check_file_on_amazon = S3KeySensor(
                task_id = "task_check_file",
                bucket_key= S3_FILE_PATH,
                bucket_name = None,
                aws_conn_id= 'aws_default',

        )

        snowflake_create_table = SnowflakeOperator(
                task_id = "define_table_for_csv",
                snowflake_conn_id= "snowflake_default",
                sql = '''
                    -- CREATE A TABLE IN THE SCHEMA
                    CREATE OR REPLACE TABLE DAFT (
                        NUM NUMBER(38,0),
                        ID NUMBER(38,0) NOT NULL,
                        LOCATION VARCHAR(255),
                        SEARCHTYPE VARCHAR(50),
                        PROPERTYTYPE VARCHAR(50),
                        TITLE VARCHAR(255),
                        AGENT_ID NUMBER(38,0),
                        AGENT_BRANCH VARCHAR(255),
                        AGENT_NAME VARCHAR(255),
                        AGENT_SELLER_TYPE VARCHAR(50),
                        BATHROOMS VARCHAR(38),
                        BEDROOMS VARCHAR(38),
                        BER VARCHAR(10),
                        CATEGORY VARCHAR(50),
                        MONTHLY_PRICE VARCHAR(50),
                        PRICE VARCHAR(50),
                        LATITUDE NUMBER(8,6),
                        LONGITUDE NUMBER(9,6),
                        PUBLISH_DATE VARCHAR(50),
                        primary key (ID)
                    );
                '''
            )

        create_snowflake_data_stage = SnowflakeOperator(
                task_id = 'create_snowflake_stage',
                snowflake_conn_id = 'snowflake_default',
                sql = f'''
                    CREATE OR REPLACE STAGE DAFT_AWS 

                    url = "s3://daftsnowflake"
                    CREDENTIALS = ( AWS_KEY_ID = 'AKIAZH7NCDYGVBSLXHHW'
                    AWS_SECRET_KEY = 'qNtlENZmxXxuGx3OOQcGAPMYAuFHB5beAQopljPD')
                    DIRECTORY = ( ENABLE = true );
                '''
            )

        copy_s3_object_in_snowflake_table = SnowflakeOperator(
                task_id = 'copy_data_to_table',
                snowflake_conn_id = 'snowflake_default',
                sql = '''
                    COPY INTO DAFT
                        FROM @DAFT_AWS
                        FILES = ('data_v1.csv')
                        FILE_FORMAT = (
                            TYPE=CSV,
                            SKIP_HEADER=1,
                            FIELD_DELIMITER=',',
                            TRIM_SPACE=FALSE,
                            FIELD_OPTIONALLY_ENCLOSED_BY='"',
                            DATE_FORMAT=AUTO,
                            TIME_FORMAT=AUTO,
                            TIMESTAMP_FORMAT=AUTO
                        )
                        ON_ERROR=ABORT_STATEMENT;
                '''
            )

        check_file_on_amazon >> snowflake_create_table >> create_snowflake_data_stage >> copy_s3_object_in_snowflake_table

    with TaskGroup("Transform") as transform:

        @task
        def preprocess_extract_table():
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id = 'snowflake_default'
            )

            df = snowflake_hook.get_pandas_df('SELECT * FROM DAFT')
            df['COUNTY'] = df.LOCATION.apply(lambda x : re.split('[._]', x)[-1])
            df['SALETYPE'] = df.SEARCHTYPE.apply(lambda x : x.split('.')[-1])
            df['PROPERTYTYPE'] = df.PROPERTYTYPE.apply(lambda x : x.split('.')[-1])
            df.rename(columns={"TITLE": "ADDRESS"}, inplace = True)
            df = df[['ID', 'ADDRESS', 'COUNTY', 'SALETYPE', 'PROPERTYTYPE', 'BATHROOMS', 'BEDROOMS', 'BER', 'CATEGORY', 'MONTHLY_PRICE', 'PRICE', 'LATITUDE', 'LONGITUDE', 'PUBLISH_DATE', 'AGENT_ID', "AGENT_BRANCH", "AGENT_NAME", "AGENT_SELLER_TYPE"]]
            
            
            return df
        
        @task
        def create_tables_and_schema_for_transformed_tables(df):
            return SnowflakeOperator(
                task_id = 'create_tables_for_transformed_data',
                snowflake_conn_id = 'snowflake_transformed',
                sql = '''
                    
                      '''
            )
            

        df = preprocess_extract_table() 
        create_tables_and_schema_for_transformed_tables(df)

    extract >> transform

    


   



