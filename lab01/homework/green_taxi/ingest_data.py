import os
import argparse
from time import time
import pandas as pd
from sqlalchemy import create_engine
import gzip
import shutil

def main(params):

    user = params.user
    password = params.password
    host = params.host
    port = params.port
    table_name = params.table_name
    db = params.db
    url = params.url
    csv_name = 'output_green.csv'

    os.system(f'wget {url} -O {csv_name}')  # Downloads data

    # # BELOW CODE IS FOR WINDOWS MACHINE (WHEN RUNNING LOCALLY)
    # os.system(f'curl -L -H "User-Agent: Mozilla/5.0" {url} -o yellow_tripdata_2021-01.csv.gz')
    # with gzip.open('yellow_tripdata_2021-01.csv.gz', 'rb') as f_in:
    #     with open(csv_name, 'wb') as f_out:
    #         shutil.copyfileobj(f_in, f_out)

    engine = create_engine(f"postgresql://{user}:{password}@{db}:{port}/{table_name}")
    iter_df = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(iter_df)
    df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
    df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')

    while True:
        try:
            start_time = time()

            df = next(iter_df)
            df['lpep_pickup_datetime'] = pd.to_datetime(df['lpep_pickup_datetime'])
            df['lpep_dropoff_datetime'] = pd.to_datetime(df['lpep_dropoff_datetime'])

            df.to_sql(name=table_name, con=engine, if_exists='append')

            end_time = time()

            print('inserted another chunk, took %.3f seconds' % (end_time - start_time))

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