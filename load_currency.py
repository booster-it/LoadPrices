import os
import urllib.request
from loguru import logger
import configparser  # импортируем библиотеку для чтения конфигов
from loguru import logger
from dotenv import load_dotenv
import configparser  # импортируем библиотеку для чтения конфигов
from connect import Sql
from _utils import timing_decorator, t

@timing_decorator
def configure_logger():
# путь к settings.ini на уровень выше
    ini_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "settings.ini"))
    config = configparser.ConfigParser()
    config.read(ini_path)

    log_level = config.get("log", "level", fallback="INFO")
    log_path = config.get("log", "path", fallback="")
    # log_format = config.get("log", "format", fallback="{time} {level} {message}")
    log_rotation = config.get("log", "rotation", fallback="1 MB")
    log_retention = config.get("log", "retention", fallback="7 days")
    log_compression = config.get("log", "compression", fallback="zip")

    log_file = os.path.join(log_path, "load_brands.log") if log_path else "load_brands.log"

    logger.remove()
    logger.add(log_file, level=log_level, rotation=log_rotation, retention=log_retention, compression=log_compression)
    logger.info(f"Настройки логирования: уровень={log_level}, путь={log_path}, файл={log_file}, ротация={log_rotation}, хранение={log_retention}, сжатие={log_compression}")

load_dotenv("..\\.env")  # Загружаем переменные окружения из .env

config = configparser.ConfigParser()  # создаём объекта парсера
config.read("..\\settings.ini")  # читаем конфиг


class CurrencyLoader:
    def __init__(self):
        self.url = config["Currency"]["LoadUrl"]
        
        self.sql = Sql(
            server=os.getenv("SERVER"),
            database=os.getenv("DATABASE"),
            username=os.getenv("USERNAMES"),
            password=os.getenv("PASSWORD")
        )
        if not self.sql.connection:
            raise Exception("Не удалось подключиться к базе данных")                        

    def fetch_data(self):
        logger.info('Получение курсов с: ' + self.url)
        req = urllib.request.Request(self.url, headers={'User-Agent': 'Mozilla/5.0 (Windows NT 6.0; WOW64; rv:24.0)'
                                                       ' Gecko/20100101 Firefox/24.0'})
        webFile = urllib.request.urlopen(req)
        return webFile.read()  # читаем данные с сайта

    def load_currency(self):
        data = self.fetch_data()
        xml_str = data.decode('windows-1251').replace('encoding="windows-1251"', '')
        logger.debug('Данные: ' + xml_str)

        if xml_str:
            if self.sql.connection:
                logger.info('Успешно подключились к базе данных')
            else:
                exit()

            logger.info('Загрузка в базу данных: начало')
            cursor = self.sql.cnxn.cursor()  # создаем курсор
            cursor.fast_executemany = True  # активируем быстрое выполнение
            cursor.execute('exec LoadCurrencyRate @XMl = ?', (xml_str,))
            cursor.commit()
            logger.info('Загрузка в базу данных: конец')

if __name__ == "__main__":
    configure_logger()
    loader = CurrencyLoader()
    loader.load_currency()
    logger.info("Загрузка завершена")
