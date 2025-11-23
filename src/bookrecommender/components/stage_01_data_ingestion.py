import os
import sys
from six.moves import urllib
import zipfile
from bookrecommender.config.configuration import AppConfiguration
from bookrecommender.logger.log import logging
from bookrecommender.exception.exception_handler import AppException

class DataIngestion:
    def __init__(self, app_config = AppConfiguration()):
        #D Data ingestion initialization
        try:
            logging.info(f"{'*'*20}Data Ingestion log started{'*'*20}")
            self.data_ingestion_config = app_config.get_data_ingestion_config()
            
        except Exception as e:
            raise AppException(e, sys) from e
        
    def download_data(self):
        # Fetch data from url
        try:
            dataset_url = self.data_ingestion_config.dataset_download_url
            zip_download_dir = self.data_ingestion_config.raw_data_dir
            os.makedirs(zip_download_dir, exist_ok=True)
            data_file_name = os.path.basename(dataset_url)
            zip_file_path = os.path.join(zip_download_dir, data_file_name)
            logging.info(f"Downloading data from {dataset_url} into file {zip_file_path}")
            urllib.request.urlretrieve(dataset_url, zip_file_path)
            logging.info(f"Download data from {dataset_url} completed")
            return zip_file_path
        
        except Exception as e:
            raise AppException(e, sys) from e
        
    def extract_zip_file(self, zip_file_path: str):
        # extract the zip file into data directory
        try:
            ingested_dir = self.data_ingestion_config.ingested_dir
            os.makedirs(ingested_dir, exist_ok=True)

            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(ingested_dir)

            logging.info(f"Extracted zip file {zip_file_path} into {ingested_dir}")

        except Exception as e:
            raise AppException(e, sys) from e
        
    def initiate_data_ingestion(self):
        try:
            zip_file_path = self.download_data()
            self.extract_zip_file(zip_file_path=zip_file_path)
            logging.info(f"{'*'*20}Data Ingestion log completed {'*'*20}")

        except Exception as e:
            raise AppException(e, sys) from e