import os
import sys
import pickle
import pandas as pd
from bookrecommender.logger.log import logging
from bookrecommender.exception.exception_handler import AppException
from bookrecommender.config.configuration import AppConfiguration

class DataTransformation:
    def __init__(self, app_config = AppConfiguration()):
        try:
            self.data_transformation_config = app_config.get_data_transformation_config()
            self.data_validation_config = app_config.get_data_validation_config()

        except Exception as e:
            raise AppException(e, sys) from e
        
    def get_data_transformer(self):
        try:
            df = pd.read_csv(self.data_transformation_config.clean_data_file_path)

            # create pivot table

            books_pivot = df.pivot_table(columns='user_id', index='title', values = 'rating')
            logging.info(f"shape of book pivot table : {books_pivot.shape}")
            books_pivot.fillna(0, inplace= True)

            # save pivot table data
            os.makedirs(self.data_transformation_config.transformed_data_dir, exist_ok=True)
            pickle.dump(books_pivot, open(os.path.join(self.data_transformation_config.transformed_data_dir, "transformed_data.pkl"), 'wb'))
            logging.info(f"saved pivot table data to {self.data_transformation_config.transformed_data_dir}")

            book_names = books_pivot.index

            # saving books titles object for web app
            os.makedirs(self.data_validation_config.serialized_objects_dir, exist_ok=True)
            pickle.dump(book_names, open(os.path.join(self.data_validation_config.serialized_objects_dir, "book_names.pkl"), 'wb'))
            logging.info(f"saved book names object to {self.data_transformation_config.transformed_data_dir}")          
            
            # saving books pivot object for web app
            os.makedirs(self.data_validation_config.serialized_objects_dir, exist_ok=True)
            pickle.dump(books_pivot, open(os.path.join(self.data_validation_config.serialized_objects_dir, "books_pivot.pkl"), 'wb'))
            logging.info(f"saved books pivot object to {self.data_transformation_config.transformed_data_dir}")          

        except Exception as e:
            raise AppException(e, sys) from e 
        

    def initaite_data_transformation(self):
        try:
            logging.info(f"{'*'*20}Data Transformation log started.{'*'*20} ")
            self.get_data_transformer()
            logging.info(f"{'*'*20}Data Transformation log completed.{'*'*20} ")
            
        except Exception as e:
            raise AppException(e, sys) from e 