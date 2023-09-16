import os
import re
import pandas as pd
import io
from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.snowflake.operators.snowflake import  SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
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

        [check_file_on_amazon ,snowflake_create_table , create_snowflake_data_stage] >> copy_s3_object_in_snowflake_table

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
            object_columns = df.select_dtypes(include=['object']).columns

            
            for col in object_columns:
                df[col] = df[col].str.replace("'", "''")
                df[col] = df[col].str.strip()  
                df[col] = df[col].str.replace('"', "'")

            df = df.convert_dtypes()

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            def check_if_object_exists(bucket_name, key):
                s3_hook = S3Hook(aws_conn_id = "aws_default")

                try:
                    s3_hook.get_key(key, bucket_name)
                    return True
                except:
                    return False
                
            s3_bucket = 'daftsnowflake'
            s3_key = 'preprocessed_data.csv'
    
            if not check_if_object_exists(s3_bucket, s3_key):
                S3CreateObjectOperator(
                    task_id="Upload-to-S3",
                    aws_conn_id= 'aws_default',
                    s3_bucket='daftsnowflake',
                    s3_key='preprocessed_data.csv',
                    data=csv_data.encode(),    
                ).execute({})
            return df
        

        @task
        def create_tables_and_schema_for_transformed_tables():
            current_dir = os.path.dirname(__file__)

            sql_file_path = os.path.join(current_dir, '..', 'include', 'sql', 'transformed_schema.sql')

            with open(sql_file_path, 'r') as sql_file:
                sql = sql_file.read()
            
            
            SnowflakeOperator(
                task_id = 'create_tables_for_transformed_data',
                snowflake_conn_id = 'snowflake_transformed',
                sql = sql,
            ).execute({})

            sql_file_path = os.path.join(current_dir, '..', 'include', 'sql', 'create_daft_table_for_transformed_schema_and_copy.sql')

            with open(sql_file_path, 'r') as sql_file:
                sql = sql_file.read()


        
        load_dimension_table_agent = SnowflakeOperator(
            task_id = "load_dimension_table_agent",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                    INSERT INTO AGENT(AGENT_ID, AGENT_BRANCH, AGENT_NAME, AGENT_SELLER_TYPE)
                    SELECT d.AGENT_ID, d.AGENT_BRANCH, d.AGENT_NAME, d.AGENT_SELLER_TYPE FROM DAFT_TRANSFORMED AS d;        
            """
        )

        load_dimension_table_property = SnowflakeOperator(
            task_id = "load_dimension_table_property",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                    INSERT INTO PROPERTY(PROPERTYTYPE, BATHROOMS, BEDROOMS, BER, CATEGORY)
                    SELECT D.PROPERTYTYPE, D.BATHROOMS, D.BEDROOMS, D.BER, D.CATEGORY FROM DAFT_TRANSFORMED AS D;

                     -- Get the last generated property_id
                    SET $last_property_id = LAST_INSERT_ID();

                    -- Update the daft_transformed table with the last property_id
                    UPDATE DAFT_TRANSFORMED
                    SET property_id = $last_property_id
                    WHERE property_id IS NULL; -- Only update rows where property_id is not already set
            """
        )

        load_dimension_table_location = SnowflakeOperator(
            task_id = "load_dimension_table_location",
            snowflake_conn_id="snowflake_transformed",
            sql = """
                    INSERT INTO LOCATION(ADDRESS, COUNTY, LATITUDE, LONGITUDE)
                    SELECT D.ADDRESS, D.COUNTY, D.LATITUDE, D.LONGITUDE FROM DAFT_TRANSFORMED AS D;
                """
        )
        
        
        @task
        def create_fact_sales():
            pass

        df = preprocess_extract_table() 
        create_tables_task = create_tables_and_schema_for_transformed_tables()    
        
        create_fact_sales = create_fact_sales()

        (df >> create_tables_task) >> [load_dimension_table_agent, load_dimension_table_location, load_dimension_table_property] >> create_fact_sales

        
    with TaskGroup("Load") as load:

        @task
        def verify():
            pass

        @task
        def email():
            pass
        
        
        verify() >> email()


    extract >> transform >> load

    


   



