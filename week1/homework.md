# Homework for Week 1

Link to the homework can be found [here](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2023/week_1_docker_sql/homework.md).

Form to submit can be found [here](https://docs.google.com/forms/d/e/1FAIpQLSfZSkhUFQOf8Novq0aWTVY9LC0bJ1zlFOKiC-aVLwM-8LdxSg/viewform)

## Question 1

1. ``docker build --help``
2. Check which option has the description "Write the image ID to the file"

**Answer**: -iidfile string

## Question 2

1. ``docker run -it  python:3.9 /bin/bash`` to run docker in interactive mode with bash as entry point
2. inside container enter ``pip list`` and see that there are three modules listed:
Package    Version
---------- -------
pip        22.0.4
setuptools 58.1.0
wheel      0.38.4

**Answer**: 3

## Question 3

1. Adjust the ingest_data.py script in order to match the column names from the January 2019 dataset.

````
#!/usr/bin/env python
# coding: utf-8

import os
import argparse

from time import time

import pandas as pd
from sqlalchemy import create_engine


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
    table_name = params.table_name
    url = params.url
    
    # the backup files are gzipped, and it's important to keep the correct extension
    # for pandas to be able to open the file
    if url.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)

    df = next(df_iter)

    df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

    df.to_sql(name=table_name, con=engine, if_exists='append')


    while True: 

        try:
            t_start = time()
            
            df = next(df_iter)

            df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
            df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)

            df.to_sql(name=table_name, con=engine, if_exists='append')

            t_end = time()

            print('inserted another chunk, took %.3f second' % (t_end - t_start))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
    parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--url', required=True, help='url of the csv file')

    args = parser.parse_args()

    main(args)
````
2. build docker image with the adjusted script by running ``docker build -t green_taxi_ingest:v001 .`` from the folder the Dockerfile is in.

3. run the dockerized script 
``URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-01.csv.gz"

docker run -it \
  --network=pg-network \
  green_taxi_ingest:v001 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=ny_taxi \
    --table_name=green_taxi_trips \
    --url=${URL}``

4. Query the database: 
``Select count(*) from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-15'
and date (lpep_dropoff_datetime) = '2019-01-15';``

**Answer:20530**

## Question 4

1. Query the data
``
Select lpep_pickup_datetime,trip_distance from green_taxi_trips
order by 2 desc;
``
2. take first result (row 1) as answer as it's ordered desc.

**Answer:** 2019-01-15 

## Question 5

1. Query the database:
``
SELECT
    SUM(CASE WHEN passenger_count = 2 THEN 1 ELSE 0 END) as "trips_with_2_passengers",
    SUM(CASE WHEN passenger_count = 3 THEN 1 ELSE 0 END) as "trips_with_3_passengers"
from green_taxi_trips
where date(lpep_pickup_datetime) = '2019-01-01';
``

**Answer**: 1282 trips with 2 passengers & 254 with 3 passengers