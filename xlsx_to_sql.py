import pandas as pd
from dotenv import load_dotenv
import os

from db import create_sql_connection

load_dotenv()

def xlsx_to_sql(dir_, conn):
    for file_ in dir_:
        print({'file_url': os.path.join(xlsx_dir_url, file_)})
        df = pd.read_excel(os.path.join(xlsx_dir_url, file_))
        print(df.columns)
        table = file_.replace(".xlsx", "")
        schema = "torneos_futbol"
        df.to_sql(table, conn, if_exists='append', schema=schema, index=False)
        print(f"Table {table} was saved successfully")

if __name__ == '__main__':
    database_url = os.getenv("DATABASE_URL")
    BASE_DIR = "C:\\Users\\PC\\Documents\\Matias\\data_projects\\torneos_primera_division_arg"
    xlsx_dir_url = BASE_DIR+"\\XLSX"
    xlsx_dir_files = os.listdir(xlsx_dir_url)
    engine = create_sql_connection(database_url)
    xlsx_to_sql(xlsx_dir_files, engine)