from asyncio.windows_events import NULL
import os, fnmatch
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import configparser  # импортируем библиотеку для чтения конфигов
from connect import Sql
from _utils import timing_decorator, t

'''
Описание файла Makes.txt (нет строки с названием столбцов, сразу идут данные)

1 столбец: Бренд в зашифрованном виде
2 столбец: Бренд в расшифрованном виде
'''

@timing_decorator
def configure_logger():
# путь к settings.ini на уровень выше
    ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.ini"))
    config = configparser.ConfigParser()
    config.read(ini_path)

    log_level = config.get("log", "level", fallback="INFO")
    log_path = config.get("log", "path", fallback="")
    # log_format = config.get("log", "format", fallback="{time} {level} {message}")
    log_rotation = config.get("log", "rotation", fallback="10 MB")
    log_retention = config.get("log", "retention", fallback="7 days")
    log_compression = config.get("log", "compression", fallback="zip")

    log_file = os.path.join(log_path, "load_brands.log") if log_path else "load_brands.log"

    logger.remove()
    logger.add(log_file, level=log_level, rotation=log_rotation, retention=log_retention, compression=log_compression)
    logger.info(f"Настройки логирования: уровень={log_level}, путь={log_path}, файл={log_file}, ротация={log_rotation}, хранение={log_retention}, сжатие={log_compression}")

load_dotenv("..\\.env")  # Загружаем переменные окружения из .env

config = configparser.ConfigParser()  # создаём объекта парсера
config.read("..\\settings.ini")  # читаем конфиг

class BrandLoader:
    def __init__(self):
        self.sql = Sql(
            server=os.getenv("SERVER"),
            database=os.getenv("DATABASE"),
            username=os.getenv("USERNAMES"),
            password=os.getenv("PASSWORD")
        )
        if not self.sql.connection:
            raise Exception("Не удалось подключиться к базе данных")


    # Функция load_makes загрузка данных с использованием pandas DataFrame
    @timing_decorator
    def load_makes(self, data, table, batchsize=500):
        logger.info('Начало загрузки данных в базу SQL')
        cursor = self.sql.cnxn.cursor()      # создаем курсор
        cursor.fast_executemany = True   # активируем быстрое выполнение

        # Создаем таблицу для временных данных       
        query = f"""
         CREATE TABLE [{table}] (       
               [Code]     varchar(10) null,  
               [Name]     varchar(60) null,  
               [Country]  varchar(60) null   
         )
         
         create index ao1 on  [{table}] (Code)
        """

        
        cursor.execute(query)  # запуск создания таблицы
        cursor.execute(f'DELETE FROM {table}')

        query = " INSERT INTO [" + table + "] ([Code], [Name], [Country]) VALUES (?, ?, ?) "

        # вставляем данные в целевую таблицу
        for i in range(0, len(data), batchsize):
            if i+batchsize > len(data):
                batch = data[i: len(data)].values.tolist()
            else:
                batch = data[i: i+batchsize].values.tolist()
       
            cursor.executemany(query, batch)

        cursor.execute("exec MakesUpdate")

        logger.info('Успешно загрузили данные в базу SQL')
        return True
    
    @timing_decorator
    def process_load_makes(self):
        directory = config["LoadPath"]["MakesFile"] 
        file_list = os.listdir(directory)  # определить список всех файлов
        for file in file_list:    
            if fnmatch.fnmatch(file, "*.txt"):       
                logger.info('Начало обработки файла {0}'.format(file));
                try:
                    df = pd.read_csv(directory+file, delimiter=",", encoding='ansi', header=None, usecols=[0,1,2], keep_default_na=False);
                    df = df.fillna("")
                      
                    self.load_makes(data=df, table='#makes', batchsize=1000)
                    
                    logger.info('Завершение обработки файла {0}'.format(file))
                except BaseException as err:
                    logger.error(err)
                        
        logger.info('Завершили импорт')

if __name__ == "__main__":
    configure_logger()
    loader = BrandLoader()
    loader.process_load_makes()
    logger.info("Загрузка завершена")
