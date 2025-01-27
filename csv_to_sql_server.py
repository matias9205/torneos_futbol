import pandas as pd
from dotenv import load_dotenv
import pyodbc
import os

from db import create_sql_connection

load_dotenv()

def transform_date(date_value:str):
    date_value_list = date_value.split(", ")
    month_mapping = {month: index for index, month in enumerate(['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre'], start=1)}
    if len(date_value_list)>0:
        only_date = date_value_list[1].split(" ")
        day, month_text, year = only_date
        month_number = month_mapping.get(month_text, None)
        if month_number is None:
            raise ValueError(f"Mes '{month_text}' no reconocido")
    return f"{year}-{month_number:02d}-{day.zfill(2)}"

def csv_to_sql_server(dir_, dir_url, conn_):
    all_matchs_data = pd.DataFrame()
    for file_ in dir_:
        print({'file_url': os.path.join(dir_url, file_)})
        df = pd.read_csv(os.path.join(dir_url, file_))
        df['date'] = df['date'].apply(transform_date)
        print(df.columns)
        all_matchs_data = pd.concat([all_matchs_data, df], ignore_index=True)
    table = 'matchs_data'
    schema = "torneos_futbol"
    print({'table_columns': all_matchs_data.columns.to_list(), 'table_count': all_matchs_data.shape[0]})
    all_matchs_data.to_sql(table, conn_, if_exists='replace', schema=schema, index=False)
    print(f"Table {table} was saved successfully")

if __name__ == '__main__':
    db_user = os.getenv('SQL_SERVER_USER')
    db_pass = os.getenv('SQL_SERVER_PASS')
    db_host = os.getenv('SQL_SERVER_HOST')
    db_name = os.getenv('SQL_SERVER_DB')
    database_url = f"mssql+pyodbc://{db_user}:{db_pass}@{db_host}/{db_name}?driver=ODBC+Driver+17+for+SQL+Server"
    engine = create_sql_connection(database_url)
    BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\torneos_primera_division_arg"
    csv_dir_url = BASE_DIR+"\\CSV"
    csv_dir_files = [file for file in os.listdir(csv_dir_url) if '.csv' in file]
    # connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={db_host};DATABASE={db_name};UID={db_user};PWD={db_pass}'
    csv_to_sql_server(csv_dir_files, csv_dir_url, engine)