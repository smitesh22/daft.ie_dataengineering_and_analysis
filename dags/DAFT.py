import os
import re
import pandas as pd
import io
from datetime import datetime

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor, S3Hook
from airflow.providers.amazon.aws.operators.s3 import S3CreateObjectOperator
from airflow.providers.snowflake.operators.snowflake import  SnowflakeOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.operators.bash import BashOperator
from airflow.models import DAG
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.operators.email import EmailOperator


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

    with TaskGroup("Load") as load:

        @task
        def preprocess_extract_table():
            snowflake_hook = SnowflakeHook(
                snowflake_conn_id = 'snowflake_default'
            )

            df = snowflake_hook.get_pandas_df('SELECT * FROM DAFT')
            df['COUNTY'] = df.LOCATION.apply(lambda x : re.split('[._]', x)[-1])
            df['SALETYPE'] = df.SEARCHTYPE.apply(lambda x : x.split('.')[-1])
            df['PROPERTYTYPE'] = df.PROPERTYTYPE.apply(lambda x : x.split('.')[-1])
            extract_numeric_value = lambda s: float(max([int(match.replace(',', '')) for match in re.findall(r'[0-9,]+', s)])) if re.findall(r'[0-9,]+', s) else 0.0
            df['PRICE'] = df['PRICE'].apply(extract_numeric_value)
            df.rename(columns={"TITLE": "ADDRESS"}, inplace = True)
            df = df[['ID', 'ADDRESS', 'COUNTY', 'SALETYPE', 'PROPERTYTYPE', 'BATHROOMS', 'BEDROOMS', 'BER', 'CATEGORY', 'MONTHLY_PRICE', 'PRICE', 'LATITUDE', 'LONGITUDE', 'PUBLISH_DATE', 'AGENT_ID', "AGENT_BRANCH", "AGENT_NAME", "AGENT_SELLER_TYPE"]]
            object_columns = df.select_dtypes(include=['object']).columns
            

            

            print(df.PRICE)
            
            """
            for col in object_columns:
                df[col] = df[col].str.replace("'", "''")
                df[col] = df[col].str.strip()  
                df[col] = df[col].str.replace('"', "'")

            """

            df = df.convert_dtypes()

            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()

            
            aws_conn_id = 'aws_default'
            s3_bucket = 'daftsnowflake'
            s3_key = 'preprocessed_data.csv'

            s3_hook = S3Hook(aws_conn_id)
            s3_hook.load_string(csv_data, s3_key, bucket_name=s3_bucket, replace=True)


        create_daft_trasform = SnowflakeOperator(
                task_id = "create_daft_trasform",
                snowflake_conn_id = "snowflake_transformed",
                sql = """
                    
                    CREATE OR REPLACE TABLE DAFT_TRANSFORMED(
                        ID INT,
                        ADDRESS STRING,
                        COUNTY STRING,
                        SALETYPE STRING,
                        PROPERTYTYPE STRING,
                        BER STRING,
                        BATHROOMS STRING,
                        BEDROOMS STRING,
                        CATEGORY STRING,
                        MONTHLY_PRICE STRING,
                        PRICE STRING,
                        LATITUDE FLOAT,
                        LONGITUDE FLOAT,
                        PUBLISH_DATE STRING,
                        AGENT_ID INT,
                        AGENT_BRANCH STRING,
                        AGENT_NAME STRING,
                        AGENT_SELLER_TYPE STRING,
                        PRIMARY KEY(ID));

                    COPY INTO DAFT_TRANSFORMED
                        FROM @AWS_DAFT
                        FILES = ('preprocessed_data.csv')
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
                    """
        )

        

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


        
        load_table_agent = SnowflakeOperator(
            task_id = "load__table_agent",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                    INSERT INTO AGENT(AGENT_ID, AGENT_BRANCH, AGENT_NAME, AGENT_SELLER_TYPE)
                    SELECT d.AGENT_ID, d.AGENT_BRANCH, d.AGENT_NAME, d.AGENT_SELLER_TYPE FROM DAFT_TRANSFORMED AS d;        
            """
        )

        load_table_property = SnowflakeOperator(
            task_id = "load_table_property",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                    INSERT INTO PROPERTY(PROPERTYTYPE, BATHROOMS, BEDROOMS, BER, CATEGORY)
                    SELECT D.PROPERTYTYPE, D.BATHROOMS, D.BEDROOMS, D.BER, D.CATEGORY FROM DAFT_TRANSFORMED AS D;
            """
        )

        load_table_location = SnowflakeOperator(
            task_id = "load_table_location",
            snowflake_conn_id="snowflake_transformed",
            sql = """
                    INSERT INTO LOCATION(ADDRESS, COUNTY, LATITUDE, LONGITUDE)
                    SELECT D.ADDRESS, D.COUNTY, D.LATITUDE, D.LONGITUDE FROM DAFT_TRANSFORMED AS D;
                """
        )
        
        load_table_sales = SnowflakeOperator(
            task_id = 'load_table_sales',
            snowflake_conn_id = "snowflake_transformed",
            sql = """
                        INSERT INTO SALES(SALE_ID, SALETYPE, MONTHLY_PRICE, PRICE, PUBLISH_DATE, AGENT_ID)
                        SELECT D.ID, D.SALETYPE, D.MONTHLY_PRICE, D.PRICE, D.PUBLISH_DATE, D.AGENT_ID FROM DAFT_TRANSFORMED AS D;
                  """
        )


        
        



        df = preprocess_extract_table() 
        create_tables_task = create_tables_and_schema_for_transformed_tables()    


        (df >> create_daft_trasform >> create_tables_task ) >> [load_table_agent, load_table_location, load_table_property] >> load_table_sales 

        
    with TaskGroup("Transform") as transform:

        transform_dimension_table_agent = SnowflakeOperator(
            task_id = "transform_dimension_table_agent",
            snowflake_conn_id = "snowflake_transformed",
            sql = """
                INSERT INTO DIMENSION_AGENT(AGENT_ID, AGENT_NAME, AGENT_BRANCH, AGENT_SELLER_TYPE) 
                SELECT A.AGENT_ID, A.AGENT_NAME, A.AGENT_BRANCH, A.AGENT_SELLER_TYPE FROM AGENT AS A; 

                SELECT AGENT_ID, AGENT_BRANCH, AGENT_SELLER_TYPE FROM DIMENSION_AGENT;

                CREATE OR REPLACE TABLE DIMENSION_AGENT AS
                SELECT DISTINCT AGENT_ID, AGENT_BRANCH, AGENT_SELLER_TYPE 
                FROM DIMENSION_AGENT;
                """

        )

        transform_dimension_table_propery = SnowflakeOperator(
            task_id = "transform_dimension_table_propery",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                INSERT INTO DIMENSION_PROPERTY(PROPERTY_ID, PROPERTYTYPE, BATHROOMS, BEDROOMS, BER, CATEGORY)
                SELECT P.PROPERTY_ID, P.PROPERTYTYPE, P.BATHROOMS, P.BEDROOMS, P.BER, P.CATEGORY FROM PROPERTY AS P;
                """

        )

        transform_dimension_table_location = SnowflakeOperator(
            task_id = "transform_dimension_table_location",
            snowflake_conn_id= "snowflake_transformed",
            sql = """
                INSERT INTO DIMENSION_LOCATION(LOCATION_ID, ADDRESS, COUNTY, LATITUDE, LONGITUDE) 
                SELECT L.LOCATION_ID, L.ADDRESS, L.COUNTY, L.LATITUDE, L.LONGITUDE FROM LOCATION L;
                """

        )

        transform_fact_table_sales = SnowflakeOperator(
            task_id = "transform_fact_table_sales",
            snowflake_conn_id = "snowflake_transformed",
            sql = """
            CREATE OR REPLACE TABLE FACT_SALES AS 
            SELECT S.SALE_ID, S.SALETYPE, S.MONTHLY_PRICE, S.PRICE, S.PUBLISH_DATE, A.AGENT_ID, P.PROPERTY_ID, L.LOCATION_ID
            FROM SALES AS S
            INNER JOIN DIMENSION_AGENT AS A ON S.AGENT_ID = A.AGENT_ID
            INNER JOIN DIMENSION_PROPERTY AS P ON S.PROPERTY_ID = P.PROPERTY_ID
            INNER JOIN DIMENSION_LOCATION AS L ON S.LOCATION_ID = L.LOCATION_ID;
                """
        )

        @task
        def verify():
            pass
        

        
        
        [transform_dimension_table_agent, transform_dimension_table_propery, transform_dimension_table_location] >> transform_fact_table_sales >> verify()


    extract >> load >> transform

    

    


   



