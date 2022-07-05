from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import airflow
from get_data import _get_data
from custom.operators import GlueTriggerCrawlerOperator
from airflow.providers.amazon.aws.operators.athena import AWSAthenaOperator
from athena_query import ticker_query
import datetime

region_name="us-east-1"
with DAG(
    dag_id='aws_tickers',
    description="DAG to colect ibov/SeP500/USD-BRL store in s3 and use anthena to get insights",
    start_date=datetime.datetime(year=2022, month=1, day=1), 
    end_date=datetime.datetime(year=2022, month=7, day=10),
    schedule_interval= '@monthly',
    default_args={
        "depends_on_past": False 
 }
) as dag :


    get_data=PythonOperator(
        task_id="get_data",
        python_callable=_get_data,
        op_kwargs={
        "s3_id": "aws_airflow_conection",
        "s3_bucket": "ibov-data-airflow-ibov",
        },
        )

    crawler = GlueTriggerCrawlerOperator(
        aws_conn_id="aws_airflow_conection",
        task_id="trigger_crawler",
        crawler_name="ibov-crawler",
        region_name="us-east-1"
        )
    
    ticker_query = AWSAthenaOperator(
        task_id="ticker_insights",
        aws_conn_id="aws_airflow_conection",
        database="airflow",
        query="""
            SELECT AVG(volume) as avg_vol,
                MAX(volume) as max_vol,
                MAX(high) as max_high,
                MAX(close) as max_close,
                MIN(close) as min_close
            FROM ibov
            WHERE symbol = '^BVSP'
        """,
        output_location=f"s3://ibov-data-insights/{datetime.datetime.now()}",
        )


    get_data >> crawler >> ticker_query