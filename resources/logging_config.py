import logging

logging.basicConfig(
    level=logging.INFO,
    filename=("config_files/app.log"),
    format="%(asctime)s - %(levelname)s -  %(message)s",
    datefmt='%d-%b-%y %H:%M:%S'
)