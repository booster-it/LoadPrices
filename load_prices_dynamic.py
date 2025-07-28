
import os
import glob
import shutil
from datetime import datetime
import time
import pandas as pd
from loguru import logger
from dotenv import load_dotenv
import configparser  # импортируем библиотеку для чтения конфигов
from connect import Sql
from _utils import timing_decorator, t

load_dotenv()  # Загружаем переменные окружения из .env  

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

    log_file = os.path.join(log_path, "load_prices_dynamic.log") if log_path else "load_prices_dynamic.log"

    logger.remove()
    logger.add(log_file, level=log_level, rotation=log_rotation, retention=log_retention, compression=log_compression)
    logger.info(f"Настройки логирования: уровень={log_level}, путь={log_path}, файл={log_file}, ротация={log_rotation}, хранение={log_retention}, сжатие={log_compression}")


class PriceLoader:
    def __init__(self):
        self.sql = Sql(
            server=os.getenv("SERVER"),
            database=os.getenv("DATABASE"),
            username=os.getenv("USERNAMES"),
            password=os.getenv("PASSWORD")
        )
        if not self.sql.connection:
            raise Exception("Не удалось подключиться к базе данных")
        
    @timing_decorator
    def get_profiles(self):
        query = """
        SELECT 
              m.MappingProfileID,
              m.FileTypeID,
              m.FilePath,
              m.DelimiterID,
              d.Brief AS DelimiterBrief,
              d.Name AS DelimiterName,
              m.Flag AS Flag,
              m.BeginRow,
              m.FileNames
         FROM tMappingProfiles m WITH (NOLOCK)
         LEFT JOIN tDelimiter d WITH (NOLOCK)
                ON d.DelimiterID = m.DelimiterID
        WHERE m.MappingTypeID = 15 
          AND m.isActive = 1
        """
        cursor = self.sql.cnxn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]

    def get_mapping_fields(self, profile_id):
        query = f"""
        SELECT 
               t.MappingProfileID,
               t.MappingGroup,
               f.FieldID,
               f.Brief AS FieldBrief,
               f.Name AS FieldName,
               f.DataType AS FieldDataType,
               t.DataType,
               t.DataValue,
               t.Flag
          FROM tMappingFields t WITH (NOLOCK)
          LEFT JOIN tFields f WITH (NOLOCK) 
                 ON f.FieldID = t.FieldID
         WHERE t.MappingProfileID = {profile_id}
        """
        cursor = self.sql.cnxn.cursor()
        cursor.execute(query)
        columns = [column[0] for column in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    @timing_decorator
    def load_prices(self, df, batchsize=100000):
        logger.info(f'{df.shape[0]} строк для загрузки в pPrice')
        # logger.info(f'{df.shape[1]} столбцов для загрузки в pPrice')
        # logger.info(f'{df[5:]}')
        cursor = self.sql.cnxn.cursor()
        cursor.fast_executemany = True

        # Удаляем текущие записи
        cursor.execute("DELETE FROM pPrice")
        
        # Подготовка SQL-запроса
        cols = list(df.columns)
        placeholders = ', '.join(['?'] * len(cols))
        colnames = ', '.join(cols)
        insert_sql = f"INSERT INTO pPrice ({colnames}) VALUES ({placeholders})"

        # data = df[cols].values.tolist()
        data = [[None if pd.isna(v) else v for v in row] for row in df[cols].values.tolist()]

        logger.info(f"Начало пакетной загрузки данных в pPrice, всего строк: {len(data)}")

        for i in range(0, len(data), batchsize):
            batch = data[i:i+batchsize]
            cursor.executemany(insert_sql, batch)
            logger.info(f"Загружено строк: {i+len(batch)}")

        logger.info("Данные загружены в pPrice")

        # Обновляем или перерасчитываем
        cursor.execute("EXEC PriceUpdate")
        logger.info("Выполнена процедура PriceUpdate")

    @timing_decorator
    def process_all_profiles(self):
        profiles = self.get_profiles()
        for profile in profiles:
            profile_id = profile["MappingProfileID"]
            file_type  = profile["FileTypeID"]
            path_mask  = profile["FilePath"]
            delimiter  = profile["DelimiterBrief"]
            delimiterName  = profile["DelimiterName"]
            has_header = ((profile.get("Flag") or 0) & 1) > 0

            mapping = self.get_mapping_fields(profile_id)
            if not mapping:
                logger.warning(f"Нет маппинга для профиля {profile_id}")
                continue

            # Словарь field -> meta info
            field_map = {
                m["FieldBrief"]: {
                    "MappingDataType": m["DataType"],
                    "FieldDataType": (m["FieldDataType"] or "").lower(),
                    "DataValue": m["DataValue"]
                }
                for m in mapping
            }
            
            # logger.info(f"Профиль field_map\n{field_map}")

            # Подготовим типы данных для pandas
            dtype_map = {}
            for field, meta in field_map.items():
                if meta["DataValue"] is not None:
                    try:
                        idx = int(meta["DataValue"]) - 1
                        ftype = meta["FieldDataType"].lower()

                        if idx >= 0:
                            if "char" in ftype or "text" in ftype or ftype == "str":
                                dtype_map[idx] = str
                            elif "float" in ftype or "real" in ftype or "decimal" in ftype or "numeric" in ftype:
                                dtype_map[idx] = float
                            elif "int" in ftype:
                                dtype_map[idx] = float
                    except Exception as e:
                        logger.warning(f"Ошибка при формировании dtype для поля {field}: {e}")

            # logger.info(f"Профиль dtype_map\n{dtype_map}")

            matched_files = glob.glob(path_mask)
            if not matched_files:
                logger.warning(f"Файлы не найдены по маске {path_mask}")
                continue

            logger.info(f"Обработка профиля {profile_id} с файлами: {matched_files}")           
            logger.info(f"Используемый разделитель: {delimiterName}")
            logger.info(f"Флаги профиля: {profile.get('Flag', 0)}")
            logger.info(f"file_type: {file_type}")
            folder = os.path.dirname(path_mask) + os.sep

            for file_path in matched_files:
                file = os.path.basename(file_path)
                try:
                    logger.info(f"Чтение файла: {file_path}")
                    if file_type == 0:
                        df_raw = pd.read_csv(
                            file_path,
                            delimiter=delimiter,
                            header=0 if has_header else None,
                            encoding="ansi",
                            low_memory=False
                        )
                    else:
                        df_raw = pd.read_excel(
                            file_path,
                            header=0 if has_header else None
                        )

                    # df_raw = df_raw.fillna("")
                    df_raw = df_raw[df_raw['DetailNum'] != '']
                    # df_raw = df_raw.where(pd.notna(df_raw), None)
                    # logger.info(f"{df_raw}")

                    df_ready = pd.DataFrame()

                    for field, meta in field_map.items():
                        dtype = meta["MappingDataType"]
                        ftype = meta["FieldDataType"].lower()
                        val   = meta["DataValue"]

                        if dtype == 0:
                            try:
                                idx = int(val) - 1
                                if idx >= df_raw.shape[1]:
                                    logger.error(f"Индекс {val} для поля '{field}' выходит за пределы столбцов")
                                    raise IndexError(f"Индекс {val} для '{field}' недопустим")
                              
                                # if ftype in ["float", "real", "decimal", "numeric", "int"]:
                                    # df_ready[field] = pd.to_numeric(df_raw.iloc[:, idx], errors="coerce")
                                # elif ftype in ["varchar", "nvarchar", "text", "char", "str"]:
                                df_ready[field] = df_raw.iloc[:, idx] #.astype(str)
                                # df_ready[field] = df_raw.iloc[:, idx].replace("", None).astype(ftype)
                            except Exception as e:
                                logger.error(f"Ошибка чтения столбца {val} для поля {field} в файле {file}: {e}")
                                raise
                        elif dtype == 1:
                            df_ready[field] = [val] * len(df_raw)

                        # Приведение к правильному типу
                        # if field in dtype_map:
                        #     try:
                        #         df_ready[field] = df_ready[field].astype(dtype_map[field])
                        #     except Exception as e:
                        #         logger.warning(f"Не удалось привести {field} к {dtype_map[field]}: {e}")

                    if "DetailNum" in df_ready.columns:
                        df_ready = df_ready[df_ready["DetailNum"].notna() & (df_ready["DetailNum"] != "")]

                    # logger.info(f"Готово к загрузке:\n{df_ready.head(5)}")
                    self.load_prices(df_ready)
                    # self.archive_file(folder, file)
                    logger.success(f"Файл обработан: {file}")

                except Exception as ex:
                    logger.error(f"Ошибка при обработке файла {file} профиля {profile_id}: {ex}")
                    continue

    @timing_decorator            
    def archive_file(self, folder, file):
        dst = os.path.join(folder, "Archive")
        if not os.path.isdir(dst):
            os.mkdir(dst)
        shutil.move(os.path.join(folder, file), os.path.join(dst, file))
        logger.info(f"Файл перемещён в архив: {file}")
        
if __name__ == "__main__":
    # configure_logger()
    loader = PriceLoader()
    loader.process_all_profiles()
    logger.info("Загрузка завершена")
