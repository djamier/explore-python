import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine
from sqlalchemy.types import DateTime, BigInteger, Boolean, Float, Integer
import os


def get_file_path() -> str:
    path = ('../dataset/sample.csv')
    return path

def get_dataframe(path :str) -> DataFrame:
    df = pd.read_csv(path, sep=',')
    return df

def transformation_data(df :DataFrame) -> DataFrame:
    df.dropna(inplace= True)

    # Convert data type
    df['VendorID'] = df['VendorID'].astype(int)
    df['passenger_count'] = df['passenger_count'].astype(int)
    df['PULocationID'] = df['PULocationID'].astype(int)
    df['DOLocationID'] = df['DOLocationID'].astype(int)
    df['payment_type'] = df['payment_type'].astype(int)

    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].replace(['N', 'Y'], [False, True])
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].astype('boolean')

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    # Rename column
    df.rename(columns={'VendorID': 'vendor_id'}, inplace= True)
    df.rename(columns={'RatecodeID': 'rate_code_id'}, inplace= True)
    df.rename(columns={'PULocationID': 'pu_location_id'}, inplace= True)
    df.rename(columns={'DOLocationID': 'do_location_id'}, inplace= True)

    return df


def get_postgres_conn() -> str:
    user = os.getenv('user')
    password = os.getenv('password')
    host = os.getenv('host')
    database = os.getenv('database')
    port = os.getenv('port')

    conn_string = f'postgresql://{user}:{password}@{host}:{port}/{database}'
    engine = create_engine(conn_string) 
    return engine


def load_to_postgres(postgres_conn: str, df:DataFrame): 
        df_schema = {
        'vendor_id': BigInteger,
        'tpep_pickup_datetime': DateTime,
        'tpep_dropoff_datetime': DateTime,
        'passenger_count': Integer,
        'trip_distance': Float,
        'rate_code_id': Float,
        'store_and_fwd_flag': Boolean,
        'pu_location_id': Integer,
        'do_location_id': Integer,
        'payment_type': Integer,
        'fare_amount': Float,
        'extra': Float,
        'mta_tax': Float,
        'tip_amount': Float,
        'tolls_amount': Float,
        'improvement_surcharge': Float,
        'total_amount': Float,
        'congestion_surcharge': Float,
        'airport_fee': Float
        }

        df.to_sql(name='from_csv', con=postgres_conn, if_exists='replace', index=False, schema='public', dtype=df_schema, method=None, chunksize=5000)


if __name__ == '__main__':
    path = get_file_path()
    df = get_dataframe(path)
    postgres_conn = get_postgres_conn()
    clean_data = transformation_data(df)
    load_to_postgres(postgres_conn, clean_data)




