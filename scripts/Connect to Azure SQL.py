
from sqlalchemy import create_engine

server = 'elt-world-bank.database.windows.net'
database = 'ELT'
username = 'CloudSA648a5ceb'
password = 'Urno9@$$'
driver = 'ODBC Driver 17 for SQL Server'

connection_string = f"mssql+pyodbc://{username}:{password}@{server}:1433/{database}?driver={driver}"
engine = create_engine(connection_string)