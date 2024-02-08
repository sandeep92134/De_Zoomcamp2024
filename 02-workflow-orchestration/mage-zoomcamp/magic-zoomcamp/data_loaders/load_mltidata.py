import io
import pandas as pd
import requests

if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

file_urls = [
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-10.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-11.csv.gz',
    'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2020-12.csv.gz'
    
]

@data_loader
def load_mltidata(file_urls, *args, **kwargs):
    """
    Template for loading data from API
    """
    data_frames = []

    taxi_dtypes = {
        'VendorID': pd.Int64Dtype(),
        # 'lpep_pickup_datetime': 'datetime64[ns]',
        # 'lpep_dropoff_datetime': 'datetime64[ns]',
        'store_and_fwd_flag': str,
        'RatecodeID': pd.Int64Dtype(),
        'PULocationID': pd.Int64Dtype(),
        'DOLocationID': pd.Int64Dtype(),
        'passenger_count': pd.Int64Dtype(),
        'trip_distance': float,
        'fare_amount': float,
        'extra': float,
        'mta_tax': float,
        'tip_amount': float,
        'tolls_amount': float,
        'ehail_fee': float,
        'improvement_surcharge': float,
        'total_amount': float,
        'payment_type': pd.Int64Dtype(),
        'trip_type': pd.Int64Dtype(),
        'congestion_surcharge': float
    }

    # Iterate over the list of file URLs
    for url in file_urls:
        # Read the data for the current file
        current_file_data = pd.read_csv(url, sep=',', compression='gzip', dtype=taxi_dtypes, parse_dates=['lpep_pickup_datetime', 'lpep_dropoff_datetime'])
        
        # Append the current file's data frame to the list
        data_frames.append(current_file_data)

    # Concatenate the data frames into a single data frame
    concatenated_data = pd.concat(data_frames, ignore_index=True)

    return concatenated_data

output_data = load_mltidata(file_urls)
print(output_data.head())

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
