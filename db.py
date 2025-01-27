import psycopg2
from sqlalchemy import create_engine, exc

def create_sql_connection(db_uri):
    engine = None
    try:
        engine = create_engine(db_uri)
        with engine.connect():
            return engine
    except exc.SQLAlchemyError as e:
        print(str(e))