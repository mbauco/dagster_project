import io
import pandas as pd
import requests
import boto3
import json
from dagster import asset

@asset(group_name="hackernews")
def airlineurl():
    airline_url = "https://think.cs.vt.edu/corgis/datasets/json/airlines/airlines.json"
    airlines = requests.get(airline_url).json()
    return airlines

@asset(group_name="hackernews")
def airportdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Code","Name"])

@asset(group_name="hackernews")
def airport(airlineurl, airportdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        # airport
        airportdf["Code"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        airportdf["Name"][item_id] = airlineurl[item_id]["Airport"]["Name"]
    return airportdf

@asset(group_name="hackernews")
def timesdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Airport","Label","Month","Month Name","Year"])

@asset(group_name="hackernews")
def times(airlineurl, timesdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        timesdf["Airport"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        timesdf["Label"][item_id] = airlineurl[item_id]["Time"]["Label"]
        timesdf["Month"][item_id] = airlineurl[item_id]["Time"]["Month"]
        timesdf["Month Name"][item_id] = airlineurl[item_id]["Time"]["Month Name"]
        timesdf["Year"][item_id] = airlineurl[item_id]["Time"]["Year"]
    return timesdf

@asset(group_name="hackernews")
def delaysdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Airport","Carrier","Late Aircraft","National Aviation System","Security","Weather"])

@asset(group_name="hackernews")
def delays(airlineurl, delaysdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        delaysdf["Airport"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        delaysdf["Carrier"][item_id] = airlineurl[item_id]["Statistics"]["# of Delays"]["Carrier"]
        delaysdf["Late Aircraft"][item_id] = airlineurl[item_id]["Statistics"]["# of Delays"]["Late Aircraft"]
        delaysdf["National Aviation System"][item_id] = airlineurl[item_id]["Statistics"]["# of Delays"]["National Aviation System"]
        delaysdf["Security"][item_id] = airlineurl[item_id]["Statistics"]["# of Delays"]["Security"]
        delaysdf["Weather"][item_id] = airlineurl[item_id]["Statistics"]["# of Delays"]["Weather"]
    return delaysdf

@asset(group_name="hackernews")
def carriersdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Airport","Names","Total"])

@asset(group_name="hackernews")
def carries(airlineurl, carriersdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        carriersdf["Airport"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        carriersdf["Names"][item_id] = airlineurl[item_id]["Statistics"]["Carriers"]["Names"]
        carriersdf["Total"][item_id] = airlineurl[item_id]["Statistics"]["Carriers"]["Total"]
    return carriersdf

@asset(group_name="hackernews")
def flightsdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Airport","Cancelled","Delayed","Diverted","On Time","Total"])

@asset(group_name="hackernews")
def flights(airlineurl, flightsdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        flightsdf["Airport"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        flightsdf["Cancelled"][item_id] = airlineurl[item_id]["Statistics"]["Flights"]["Cancelled"]
        flightsdf["Delayed"][item_id] = airlineurl[item_id]["Statistics"]["Flights"]["Delayed"]
        flightsdf["Diverted"][item_id] = airlineurl[item_id]["Statistics"]["Flights"]["Diverted"]
        flightsdf["On Time"][item_id] = airlineurl[item_id]["Statistics"]["Flights"]["On Time"]
        flightsdf["Total"][item_id] = airlineurl[item_id]["Statistics"]["Flights"]["Total"]
    return flightsdf

@asset(group_name="hackernews")
def minutesdf(airlineurl) -> pd.DataFrame:
    return pd.DataFrame(airlineurl, columns=["Airport","Carrier","Late Aircraft","National Aviation System","Security","Total","Weather"])

@asset(group_name="hackernews")
def minutes(airlineurl, minutesdf: pd.DataFrame) -> pd.DataFrame:
    lenght = len(airlineurl)
    for item_id in range(lenght):
        minutesdf["Airport"][item_id] = airlineurl[item_id]["Airport"]["Code"]
        minutesdf["Carrier"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["Carrier"]
        minutesdf["Late Aircraft"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["Late Aircraft"]
        minutesdf["National Aviation System"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["National Aviation System"]
        minutesdf["Security"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["Security"]
        minutesdf["Total"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["Total"]
        minutesdf["Weather"][item_id] = airlineurl[item_id]["Statistics"]["Minutes Delayed"]["Weather"]
    return minutesdf
    

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_airport(airport: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'airport/' + f"airport"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            airport.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(airport)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_times(times: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'times/' + f"times"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            times.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(times)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_delays(delays: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'delays/' + f"delays"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            delays.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(delays)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_carries(carries: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'carries/' + f"carries"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            carries.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(carries)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_flights(flights: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'flights/' + f"flights"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            flights.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(flights)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))

@asset(group_name="hackernews", required_resource_keys={"s3"})
def upload_minutes(minutes: pd.DataFrame) -> None:

    # Get API Keys
    content = open('D:/dagster_project/config.json')
    config = json.load(content)
    access_key = config['access_key']
    secret_access_key = config['secret_access_key']

    try:
        rows_imported = 0
        # save to s3
        upload_file_bucket = 'dagstertest2'
        upload_file = 'minutes/' + f"minutes"
        filepath =  upload_file + ".csv"
        
        s3_client = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key,region_name='us-east-2')
        with io.StringIO() as csv_buffer:
            minutes.to_csv(csv_buffer, index=False, header=False)

            response = s3_client.put_object(
                Bucket=upload_file_bucket, Key=filepath, Body=csv_buffer.getvalue()
            )

            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")

            if status == 200:
                print(f"Successful S3 put_object response. Status - {status}")
            else:
                print(f"Unsuccessful S3 put_object response. Status - {status}")
            rows_imported += len(minutes)
            print("Data imported successful")

    except Exception as e:
        print("Data load error: " + str(e))