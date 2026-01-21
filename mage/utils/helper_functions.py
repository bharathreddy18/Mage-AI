from datetime import timedelta
from pathlib import Path
import boto3
import os
from dotenv import load_dotenv

load_dotenv()

def get_prj_dir():
    data_dir = Path(__file__).parents[1]
    data_dir.mkdir(parents=True, exist_ok=True)
    return data_dir

def get_prj_data_dir():
    data_dir = Path(__file__).parents[1] / "data"
    data_dir.mkdir(parents=True, exist_ok=True)
    # Create directories
    for subdir in ["raw", "interim", "processed", "external"]:
        (data_dir / subdir).mkdir(parents=True, exist_ok=True)
    return data_dir

def get_previous_week_window(execution_date):
    days_to_subtract = execution_date.weekday() + 7
    start_date = (execution_date - timedelta(days=days_to_subtract)).date()
    end_date = start_date + timedelta(days=6)
    return start_date, end_date

def s3():
    s3_client = boto3.client(
        's3',
        endpoint_url=os.getenv('WASABI_ENDPOINT'),
        aws_access_key_id=os.getenv('WASABI_ACCESS_KEY'),
        aws_secret_access_key=os.getenv('WASABI_SECRET_ACCESS_KEY')
    )
    return s3_client

state_code_mapping={
    "Andhra Pradesh": '28',
    "Arunachal Pradesh": '12',
    "Bihar": '10',
    "Chhattisgarh": '22',
    "Goa": '30',
    "Gujarat": '24',
    "Haryana": '6',
    "Himachal Pradesh": '2',
    "Jammu And Kashmir": '1',
    "Karnataka": '29',
    "Kerala": '32',
    "Madhya Pradesh": '23',
    "Maharashtra": '27',
    "Manipur": '14',
    "Meghalaya": '17',
    "Delhi": '7',
    "Nagaland": '13',
    "Odisha": '21',
    "Puducherry": '34',
    "Punjab": '3',
    "Rajasthan": '8',
    "Tamil Nadu": '33',
    "Telangana": '36',
    "Tripura": '16',
    "Uttar Pradesh": '9',
    "Uttarakhand": '5',
    "West Bengal": '19',
    "Chandigarh": '4',
    "Assam": '18',
}