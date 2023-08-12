from os import environ, getcwd, listdir
from time import sleep
import datetime
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text, Engine
from sqlalchemy.exc import OperationalError
from config import (SQL_FOLDER, QUERY_AGGREGATE, QUERY_CREATE_TABLE, QUERY_DELETE_HOUR, 
    TARGET_TABLE, TARGET_SCHEMA,
    PERIODICITY)


print('Waiting for the data generator...')
sleep(20)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine = create_engine(environ["MYSQL_CS"], pool_pre_ping=True, pool_size=10)
        mysql_engine_root = create_engine(environ["MYSQL_CS_ROOT"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL and MySQL successful.')

# Write the solution here

# In production i would solve that type of tasks with scheduling in Airflow, and probably using pyspark
# But, lets try to solve it without any additional tools 

def get_data(day_id: datetime, 
             hour_id: int, 
             engine: Engine, 
             query_path: Path) -> pd.DataFrame:

    # at first download needed aggregate
    with engine.connect() as con:
        df = pd.read_sql(text(query_path.read_text().format(**{'day_id': day_id, 'hour_id': hour_id})),
            con=con)
    
    print('data readed has len {} at {}'.format(len(df), processing_time))

    return df


def load_data(df: pd.DataFrame,
              day_id: datetime, 
              hour_id: int, 
              engine: Engine, 
              engine_root: Engine, 
              query_create_path: Path,
              query_delete_path: Path) -> None:

    # to not play with permissions, i just can use root for creating table
    with engine_root.connect() as con:
        # create target table, if we don't have one
        con.execute(text(query_create_path.read_text().format(**{'target_schema': TARGET_SCHEMA,
                                                                 'target_table':TARGET_TABLE})))

        # drop processed hour for idempotency
        con.execute(text(query_delete_path.read_text().format(**{'day_id': day_id,
                                                                 'hour_id': hour_id,
                                                                 'target_schema': TARGET_SCHEMA,
                                                                 'target_table':TARGET_TABLE})))

        print('data dropped (if exists)')

    # with engine.connect() as con:
        
        # and finally load needed data
        df.to_sql(name=TARGET_TABLE, schema=TARGET_SCHEMA, con=con, if_exists='append', chunksize='1000')
    
    print('data loaded')


while True:

    # i'm not sure, if that should be near realtime processing, or not. 
    # So i choose repeatable batch processing, because it will be more strict and reliable.
    # we will repeat ETL process every 10 minutes, for example.
    # Because in real life we can get some late data
    # So it can be useful to refresh all our aggregated data throughout next hour after event were happend
   
    # get day and hour for hour, that we would like to process
    processing_time = datetime.datetime.now() - datetime.timedelta(hours=1)
    processing_day_id = processing_time.date()
    processing_hour_id = processing_time.hour


    df = get_data(day_id=processing_day_id, 
                  hour_id=processing_hour_id, 
                  engine=psql_engine, 
                  query_path=Path(SQL_FOLDER, QUERY_AGGREGATE))

    load_data(df=df,
              day_id=processing_day_id, 
              hour_id=processing_hour_id, 
              engine=mysql_engine,
              engine_root=mysql_engine_root,
              query_create_path=Path(SQL_FOLDER, QUERY_CREATE_TABLE),
              query_delete_path=Path(SQL_FOLDER, QUERY_DELETE_HOUR))

    sleep(PERIODICITY)
