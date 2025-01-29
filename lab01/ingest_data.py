import os
from sqlalchemy import create_engine
import pandas as pd
from time import time
import argparse
import gzip
import shutil

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    csv_name = 'output.csv'

    # BELOW CODE IS FOR WINDOWS MACHINE (WHEN RUNNING LOCALLY)
    # os.system(f'curl -L -H "User-Agent: Mozilla/5.0" {url} -o yellow_tripdata_2021-01.csv.gz')
    # with gzip.open('yellow_tripdata_2021-01.csv.gz', 'rb') as f_in:
    #     with open(csv_name, 'wb') as f_out:
    #         shutil.copyfileobj(f_in, f_out)

    # CODE BELOW FOR LINUX MACHINE (WHICH WILL BE RUNNING IN THE CONTAINER)
    os.system(f'wget {url} -O {csv_name}')

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df_first_chunk = next(df_iter)
    df_first_chunk['tpep_pickup_datetime'] = pd.to_datetime(df_first_chunk['tpep_pickup_datetime'])
    df_first_chunk['tpep_dropoff_datetime'] = pd.to_datetime(df_first_chunk['tpep_dropoff_datetime'])

    df_first_chunk_head = df_first_chunk.head(0)

    # We ipload the first 100000 separately because each time we run next(df_iter) we skip to the nest iteration. Since we already done it once
    # It is necessary to log this first iteration. Also it is good to check that we are uploading correctly.
    df_first_chunk_head.to_sql(name=table_name, con=engine, if_exists='replace') # This code creates the headers in SQL DB
    df_first_chunk.to_sql(name=table_name, con=engine, if_exists='append')  # This code inserts the first 100000 data


    while True:
        
        try: 
            t_start = time()

            df = next(df_iter)
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f seconds' % (t_end - t_start))

        except StopIteration:
            print("Finnished ingesting data into postgres database.")
            break

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')
    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table-name', help='name of the table where we will write results to')
    parser.add_argument('--url', help='url of the csv file')

    args = parser.parse_args()

    main(args)