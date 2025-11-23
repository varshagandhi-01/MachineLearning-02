from bookrecommender.exception.exception_handler import AppException
from bookrecommender.logger.log import logging
import sys 

try:

    a = 3/0

except Exception as e:
    logging.info(e)
    raise AppException(e, sys) from e