import os
import sys
import ast
import pandas as pd
import pickle
from bookrecommender.logger.log import logging
from bookrecommender.exception.exception_handler import AppException
from bookrecommender.config.configuration import AppConfiguration

class DataValidation:
    def __init__(self, app_config = AppConfiguration()):
        try:
            self.data_validation_config = app_config.get_data_validation_config()
        except Exception as e:
            raise AppException(e, sys) from e
        
    def process_data(self):
        try:
            ratings = pd.read_csv(self.data_validation_config.ratings_csv_file, sep=";", on_bad_lines='skip', encoding='latin-1')
            books = pd.read_csv(self.data_validation_config.books_csv_file, sep=";", on_bad_lines='skip', encoding='latin-1')

            logging.info(f"shape of ratings data file: {ratings.shape}")
            logging.info(f"shape of books data file: {books.shape}")

            books = books[['ISBN', 'Book-Title', 'Book-Author', 'Year-Of-Publication', 'Publisher','Image-URL-L']]

            books.rename(columns={"Book-Title":"title", 
                                  "Book-Author": "author", 
                                  "Year-Of-Publication":"year", 
                                  "Publisher":"publisher", 
                                  "Image-URL-L": "image-url"}, inplace=True)
            
            ratings.rename(columns={"User-ID":'user_id',
                                'Book-Rating':'rating'},inplace=True)
            
            #looks for users with atleast 200 ratings'
            x = ratings["user_id"].value_counts() > 200
            y = x[x].index

            ratings = ratings[ratings['user_id'].isin(y)]

            ratings_with_books = ratings.merge(books, on = 'ISBN')
            number_rating = ratings_with_books.groupby('title')['rating'].count().reset_index()
            number_rating.rename(columns={'rating':'num_of_rating'}, inplace=True)
            
            final_rating = ratings_with_books.merge(number_rating, on='title')

            #only consider books with 50 or more ratings
            final_rating = final_rating[final_rating['num_of_rating'] >= 50]

            #now remove duplicates
            final_rating.drop_duplicates(['user_id', 'title'], inplace=True)
            logging.info(f"shape of final rating: {final_rating.shape}")

            #save this clean dataset
            os.makedirs(self.data_validation_config.clean_data_dir, exist_ok=True)
            final_rating.to_csv(os.path.join(self.data_validation_config.clean_data_dir, 'clean_data.csv'), index = False)
            logging.info(f"clean data saved to {self.data_validation_config.clean_data_dir}")
            
            #save final ratings object for webapp
            os.makedirs(self.data_validation_config.serialized_objects_dir, exist_ok=True)
            pickle.dump(final_rating, open(os.path.join(self.data_validation_config.serialized_objects_dir, 'final_rating.pkl'),'wb'))
            logging.info(f"saved final rating serialized object to {self.data_validation_config.serialized_objects_dir}")

        except Exception as e:
            raise AppException(e, sys) from e
        
    def initiate_data_validation(self):
        try:
            logging.info(f"{'*'*20}Data Validation log started.{'*'*20} ")
            self.process_data()
            logging.info(f"{'*'*20}Data Validation log completed.{'*'*20} ")

        except Exception as e:
            raise AppException(e, sys) from e