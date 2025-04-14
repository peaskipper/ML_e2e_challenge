import logging
from pathlib import Path

# Define a custom log format with date and time formatting
reformat = {
    'log_format':"%(asctime)s - %(levelname)s - %(module)s - %(lineno)d - %(message)s",
    'date_format':"%Y-%m-%d %H:%M:%S"
}

# Specify the default path for the log file
log_file_path = str(Path(__file__).parent / '\\app.log')

# Configure the basic logging settings with the custom file path
def logger_setup(log_file_path:str=log_file_path, reformat:bool=False):
    if reformat:
        log_format = reformat['log_format']
        date_format = reformat['date_format']
        logging.basicConfig(filename=log_file_path, format=log_format, datefmt=date_format, level=logging.DEBUG)
    else:
        logging.basicConfig(filename=log_file_path, level=logging.DEBUG)


# Create a logger object
logger = logging.getLogger("app_logger")

# Configure the logger level to capture messages of INFO level and above
logger.setLevel(logging.INFO)


logger_setup()
logger.info('teehee')