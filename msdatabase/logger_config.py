import os
import logging
from datetime import datetime

def get_logger(class_name: str):
    date = datetime.now().strftime("%d-%m-%Y")
    file_name = f"{date}_logs.log"
    log_dir = 'logs'
    full_file_path = os.path.join(log_dir, file_name)
    
    os.makedirs(log_dir, exist_ok=True)
    
    file_handler = logging.FileHandler(full_file_path, mode='a', encoding='utf-8')
    
    logger = logging.getLogger(class_name)
    if not logger.hasHandlers():
        logger.setLevel(logging.INFO)
        
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%d-%m-%Y %H:%M:%S"
        )
        
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
    return logger