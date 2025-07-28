import pyodbc
#import pymssql
from loguru import logger

class Sql:
    """Sql - класс подключения к базе данных
    """
    connection = False

    def __init__(self, server, database, username, password):        
        """__init__ - инициализация подключения к базе данных """
        try:
            # ODBC Driver 17 for SQL Server
            # SQL Server Native Client 11.0
            self.cnxn = pyodbc.connect("Driver={ODBC Driver 17 for SQL Server};"
                                        "Server="+server+";"
                                        "Database="+database+";"
                                        "UID="+username+";"
                                        "PWD="+password+";"
                                        "Trusted_Connection=no;",
                                        autocommit=True
                                        )
            self.connection = True
        except BaseException as err:
            logger.error("Ошибка подключения к базе данных")
            logger.error(err)
            self.connection = False
            
    def close(self):
        if self.connection:
            self.cnxn.close()           
