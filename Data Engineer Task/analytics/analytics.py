from os import environ
from time import sleep
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
import logging
import psycopg2
import pandas as pd
from geopy.distance import distance
import json
import pymysql

print('Waiting for the data generator...')
sleep(100)
print('ETL Starting...')

while True:
    try:
        psql_engine = create_engine(environ["POSTGRESQL_CS"], pool_pre_ping=True, pool_size=10)
        break
    except OperationalError:
        sleep(0.1)
print('Connection to PostgresSQL successful.')

# Write the solution here



# Set up logging
logging.basicConfig(filename='logs.log', level=logging.DEBUG, format='%(asctime)s:%(levelname)s:%(message)s')

# Extract Lon and Lat
def get_lat_lon(location):
    location_dict = json.loads(location)
    return pd.Series([location_dict['latitude'], location_dict['longitude']])

# Calculate the total distance traveled by each device in each hour
def calculate_distance(group):
    lats = group['latitude'].tolist()
    longs = group['longitude'].tolist()
    return sum([distance((lats[i], longs[i]), (lats[i+1], longs[i+1])).km for i in range(len(lats)-1)])

try:
    # Establish a connection to the MySQL database
    db_uri = environ["MYSQL_CS"]
    engine = create_engine(db_uri)
    connection = engine.connect()



    # Read data from the Postgres table
    df = pd.read_sql('select * from devices', con=psql_engine)
    
    # Extract Lat and Lon from the location column
    df[['latitude', 'longitude']] = df['location'].apply(lambda x: get_lat_lon(x))
    df.drop('location', axis=1, inplace=True)

    # Convert time column to datetime format
    df['time'] = pd.to_datetime(df['time'], unit='s')

    # Group the data by device and hour
    grouped = df.groupby([pd.Grouper(key='time', freq='H'), 'device_id'])

    # Calculate the maximum temperature for each device and hour
    max_temps = grouped['temperature'].max()

    # Count the number of data points for each device and hour
    count_data_points = grouped.size()

    # Calculate the total distance traveled for each device in each hour
    total_distance = round(grouped.apply(calculate_distance),0)

    # Combine the results into a single DataFrame
    result = pd.concat([max_temps, count_data_points, total_distance], axis=1)
    result.columns = ['max_temperature', 'data_point_count', 'total_distance']
    result = result.reset_index()

    # Log metadata about the results
    logging.info(f"Rows in result dataframe: {len(result.index)}")
    print(f"Rows in result dataframe: {len(result.index)}")
    logging.info(f"Columns in result dataframe: {', '.join(result.columns)}")
    print(f"Columns in result dataframe: {', '.join(result.columns)}")
    logging.info(f"Devices in result dataframe: {len(result['device_id'].unique())}")
    print(f"Devices in result dataframe: {len(result['device_id'].unique())}")

    # Write the results to the MySQL table
    table_name = 'devices_agg_data'
    result.to_sql(table_name, engine, if_exists='replace', index=False)

    # Read the results back from the MySQL table
    df_test = pd.read_sql(f"SELECT * FROM {table_name}", con=engine)
    
    # Log metadata about the test dataframe
    logging.info(f"Rows in test dataframe: {len(df_test.index)}")
    print(f"Rows in test dataframe: {len(df_test.index)}")
    logging.info(f"Columns in test dataframe: {', '.join(df_test.columns)}")
    print(f"Columns in test dataframe: {', '.join(df_test.columns)}")
    logging.info(f"Devices in test dataframe: {len(df_test['device_id'].unique())}")
    print(f"Devices in test dataframe: {len(df_test['device_id'].unique())}")

except Exception as e:
    logging.error(f"Error occurred: {e}")