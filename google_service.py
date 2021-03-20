
import os
import logging
import io
import re
from datetime import datetime, timedelta, date
import gzip
import tarfile
import boto3
from botocore.exceptions import ClientError
import pandas as pd
import json
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
import httplib2
import json
import pyarrow as pa
import pyarrow.parquet as pq

# put in Docker 
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from dotenv import load_dotenv
import os.path
dev_env = os.path.exists('.env')
if dev_env:
    logger.info('Loading local dev ENV vars')
    load_dotenv()

def get_secret_path():
    """Use Secret ARN to get secret binary

    Returns:
      creates a .p12 file with the secret binary to use for Google API Auth token exchanges
    """
    logger.info('Creating key file')
    try:
        sm_client = boto3.client('secretsmanager')
        secret_name = os.getenv('SECRET_NAME')
        logger.info(f'SECRET_NAME: {secret_name}')
        response = sm_client.get_secret_value(SecretId=secret_name)
        f = open("key.p12", "wb")
        f.write(response["SecretBinary"])
        f.close()
        return "key.p12"
        
    except ClientError as e:
        logging.error(e)
        return False    

# Google Api Admin Reports Keys
service_account_email = os.getenv('SERVICE_ACCOUNT_EMAIL')
user_email = os.getenv('USER_EMIAL')
target_bucket = os.getenv('TARGET_BUCKET')
    
def main():
   logger.info('creating reports service!')
   service_obj = create_reports_service(user_email)  
   logger.info('Getting 7 parameters from Admin SDK Reports API customerUsageReports')
   reports_date = (datetime.now() - timedelta(6)).strftime('%Y-%m-%d') # iso format yyyy-mm-dd
   parameters='meet:num_30day_active_users, gmail:num_30day_active_users, drive:num_monthly_active_users,drive:num_sharers, meet:num_30day_active_users, accounts:drive_used_quota_in_mb, accounts:gmail_used_quota_in_mb, accounts:gplus_photos_used_quota_in_mb'
   service_response_data_df = gather_data(service_obj, parameters, reports_date)
   prep_df_data(service_response_data_df)
   assert send_file_to_s3(reports_date)
   logger.info("...file sent to s3")


def gather_data(service_obj, params, reports_date):
    logger.info('Loop over params to gather with api  call and convert dict to df')
    data_points_list = params.split(", ")
    reports_data = []
    for i in range(len(data_points_list)):
        res = service_obj.customerUsageReports().get(date=reports_date,  parameters=data_points_list[i]).execute()  
        # parse/build data 
        customer_id = res["usageReports"][0]["entity"]["customerId"]
        reports_data_dict = res["usageReports"][0]["parameters"][0]
        reports_data_dict["reports_date"] = reports_date
        reports_data_dict["customer_id"] = customer_id
        reports_data.append(reports_data_dict)
        logger.info(f'reports_data: {reports_data}')  
    return pd.DataFrame.from_dict(reports_data, orient='columns')

def prep_df_data(df):
    logger.info('Prep Data for s3')
    logger.info(f'df: {df}')
    # Convert DataFrame to Parquet File
    table = pa.Table.from_pandas(df)
    pq.write_table(table, 'google_admin_api_data.parquet',compression='GZIP')


def send_file_to_s3(reports_date):
    """Upload a file to an S3 bucket

    Returns:
      True if file was uploaded, else False
    """
    logger.info(f'Sending to parquet file {target_bucket}...')
    s3 = boto3.client('s3')
    date_partition = reports_date.split("-")

    try:
        with open('google_admin_api_data.parquet', "rb") as f:
            s3.upload_fileobj(f, target_bucket, f'GOOGLE_SERVICES/{date_partition[0]}/{date_partition[1]}/{date_partition[2]}/google_admin_api_data.parquet')
    except ClientError as e:
        logging.error(e)
        return False
    return True


def create_reports_service(user_email): # add user email here 
    """Build and returns an Admin SDK Reports service object authorized with the service accounts
    that act on behalf of the given user.

    Args:
      user_email: The email of the user. Needs permissions to access the Admin APIs.
    Returns:
      Admin SDK reports service object.
    """
   
    logger.info('Getting secrets file')
    credentials = ServiceAccountCredentials.from_p12_keyfile(
        service_account_email,
        get_secret_path(),
        scopes=['https://www.googleapis.com/auth/admin.reports.usage.readonly'])

    credentials = credentials.create_delegated(user_email)

    http = httplib2.Http()
    http = credentials.authorize(http)
    service  = build('admin', 'reports_v1', http=http, cache_discovery=False)
    logger.info('build reports service obj with auth DONE')
    return service


if __name__ == '__main__':
    try:
        main()
        logger.info('Removing key file')
        os.remove("key.p12")
        logger.info('Processing for available admin reports data is complete')
    except BaseException as e:
        logger.exception('Error during processing of admin reports data')



