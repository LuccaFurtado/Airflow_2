
from tempfile import TemporaryDirectory
from yahooquery import Ticker
import calendar
import datetime
import airflow
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import logging
from os import path

def _get_data(s3_id, s3_bucket,**context):
    year = context["execution_date"].year
    month = context["execution_date"].month
    start_date = f"{str(year)}-{str(month)}-01"
    end_date = f"{str(year)}-{str(month)}-{calendar.monthrange(year,month)[1]}"
    logging.info(f"Getting data for {month}/{year}")
    print(s3_id,s3_bucket)
    ticker = Ticker(["^BVSP","^GSPC","USDBRL=X"])
    data = ticker.history(start=start_date, end= end_date).reset_index()
    logging.info(f"Data colected with {data.shape[0]} samples")
    
    with TemporaryDirectory() as temporary_dir:
        temporary_path = path.join(temporary_dir, f"data.csv")
        data.to_csv(temporary_path, index=False)

        logging.info(f"Uploading IBOV/{month}-{year}.csv to s3")

        s3_hook = S3Hook(s3_id)
        s3_hook.load_file( 
            temporary_path,
            key=f"ibov/{year}/{month}.csv",
            bucket_name=s3_bucket,
            replace=True,
            )

