from datetime import datetime
import logging
import os

def get_logger(class_name: str):
    # Criação do manipulador de log rotativo com base na data
    date = datetime.now().strftime("%d-%m-%Y")
    file_name = f"{date}_logs.log"
    log_dir = 'logs'  # Diretório para os arquivos de log
    full_file_path = os.path.join(log_dir, file_name)
    
    # Cria o diretório se não existir
    os.makedirs(log_dir, exist_ok=True)
    
    # Configuração do FileHandler com codificação 'utf-8'
    file_handler = logging.FileHandler(full_file_path, mode='a', encoding='utf-8')
    
    logger = logging.getLogger(class_name)
    if not logger.hasHandlers():  # Evitar múltiplos handlers
        logger.setLevel(logging.INFO)  # Nível de logging padrão
        
        # Define os padrões do Log
        formatter = logging.Formatter(
            fmt="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
            datefmt="%d-%m-%Y %H:%M:%S"
        )
        
        # Configura o handler do arquivo e do console
        file_handler.setFormatter(formatter)
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)
        
    return logger