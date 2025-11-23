import os
import sys
from bookrecommender.logger.log import logging
from bookrecommender.utils.utils import read_yaml_file
from bookrecommender.exception.exception_handler import AppException
from bookrecommender.entity.config_entity import (DataIngestionConfig, DataValidationConfig)
from bookrecommender.constants import *

class AppConfiguration:
    def __init__(self, config_file_path = CONFIG_FILE_PATH):
        try:
            self.config_info = read_yaml_file(file_path=config_file_path)

        except Exception as e:
            raise AppException(e, sys) from e

    def get_data_ingestion_config(self) -> DataIngestionConfig:
        try:
            data_ingestion_config = self.config_info['data_ingestion_config']
            artifacts_dir = self.config_info['artifacts_config']['artifacts_dir']
            dataset_dir = data_ingestion_config['dataset_dir']

            ingested_data_dir = os.path.join(artifacts_dir, dataset_dir, data_ingestion_config['ingested_dir'])
            raw_data_dir = os.path.join(artifacts_dir, dataset_dir, data_ingestion_config['raw_data_dir'])

            response = DataIngestionConfig(
                dataset_download_url = data_ingestion_config['dataset_download_url'],
                raw_data_dir = raw_data_dir,
                ingested_dir = ingested_data_dir
            )

            logging.info(f"Data ingestion Config: {response}")
            return response
        
        except Exception as e:
            raise AppException(e, sys) from e         


    def get_data_validation_config(self) -> DataValidationConfig:
        try:
            data_validation_config = self.config_info['data_validation_config']
            data_ingestion_config = self.config_info['data_ingestion_config']
            dataset_dir = data_ingestion_config['dataset_dir']
            artifacts_dir = self.config_info['artifacts_config']['artifacts_dir']
            books_csv_file = data_validation_config['books_csv_file']
            ratings_csv_file = data_validation_config['ratings_csv_file']

            books_csv_file_dir = os.path.join(artifacts_dir, dataset_dir, data_ingestion_config['ingested_dir'], books_csv_file)
            ratings_csv_file_dir = os.path.join(artifacts_dir, dataset_dir, data_ingestion_config['ingested_dir'], ratings_csv_file)
            clean_data_path = os.path.join(artifacts_dir, dataset_dir, data_validation_config['clean_data_dir'])
            serialized_objects_dir = os.path.join(artifacts_dir, data_validation_config['serialized_objects_dir'])

            response = DataValidationConfig(
                clean_data_dir=clean_data_path,
                books_csv_file=books_csv_file_dir,
                ratings_csv_file=ratings_csv_file_dir,
                serialized_objects_dir=serialized_objects_dir
            )

            logging.info(f"Data validation config: {response}")
            return response

        except Exception as e:
            raise AppException(e, sys) from e