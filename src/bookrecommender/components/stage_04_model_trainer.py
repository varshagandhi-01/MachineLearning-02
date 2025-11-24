import os
import sys
import pickle
from sklearn.neighbors import NearestNeighbors
from scipy.sparse import csr_matrix
from bookrecommender.logger.log import logging
from bookrecommender.config.configuration import AppConfiguration
from bookrecommender.exception.exception_handler import AppException

class ModelTrainer:
    def __init__(self, app_config = AppConfiguration()):
        try:
            self.model_trainer_config = app_config.get_model_trainer_config()

        except Exception as e:
            raise AppException(e, sys) from e 

    def train(self):
        try:
            # load pivot data
            book_pivot = pickle.load(open(self.model_trainer_config.transformed_data_file_dir, 'rb'))
            book_sparse = csr_matrix(book_pivot)

            model = NearestNeighbors(algorithm='brute')
            model.fit(book_sparse)

            # save the model
            os.makedirs(self.model_trainer_config.trained_model_dir, exist_ok=True)
            file_name = os.path.join(self.model_trainer_config.trained_model_dir, self.model_trainer_config.trained_model_name)
            pickle.dump(model, open(file_name, 'wb'))
            logging.info(f"saved model to {file_name}")

        except Exception as e:
            raise AppException(e, sys) from e 
        
    def initiate_model_trainer(self):
        try:
            logging.info(f"{'*'*20}Model Trainer log started{'*'*20} ")
            self.train()
            logging.info(f"{'*'*20}Model Trainer log completed{'*'*20} ")

        except Exception as e:
            raise AppException(e, sys) from e 