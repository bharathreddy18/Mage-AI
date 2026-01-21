import os
import boto3
import shutil
from pathlib import Path
from utils.helper_functions import get_prj_data_dir, s3

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


@data_exporter
def export_data(data, *args, **kwargs):
    bucket_name = os.getenv('WASABI_BUCKET')
    prefix = os.getenv('WASABI_PREFIX')

    s3_client = s3()

    data_dir = get_prj_data_dir()
    folders = ['external', 'raw', 'interim', 'processed']

    for folder in folders:
        local_path = Path(data_dir) / folder

        if not local_path.exists():
            print(f"Directory {local_path} does not exist, skipping..")
            continue
        
        for file in local_path.glob('*.*'):
            if file.suffix not in ['.parquet', '.csv']:
                continue

            file_name = file.name

            s3_key = f"{prefix}/{folder}/{file_name}"

            try:
                print(f"Uploading {file_name} to wasabi://{bucket_name}/{s3_key}....")

                s3_client.upload_file(
                    str(file),
                    bucket_name,
                    s3_key
                )
            except Exception as e:
                print(f"Failed to upload {file_name}: {str(e)}")
                raise e

    print("All uploads successful. Cleaning up local data directory...")
    try:
        for folder in folders:
            folder_path = Path(data_dir) / folder
            if folder_path.exists():
                shutil.rmtree(folder_path)

                folder_path.mkdir(parents=True, exist_ok=True)
                print(f"Cleared local folder: {folder}")
                
    except Exception as e:
        print(f"Cleanup failed but data is safe in Wasabi: {e}")
        raise e

    return data