import json
import pandas as pd
from datetime import datetime

def lambda_handler(event, context):

    bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']
    path = f's3://{bucket}/{file}'
    df = pd.read_csv(path)
    
    #format file
    df = df.rename(columns=dict(map(lambda x: (x,x.lower().replace(" ", "_")),df.columns)))
    df["incident_date"] = pd.to_datetime(df["incident_date"]).dt.date
    df.zipcode = df.zipcode.astype(str)
    df["station_area"] = df["station_area"].astype(str)
    df.box = df.box.astype(str)
    df["no_flame_spead"] = df["no_flame_spead"].astype(str)

    #upload STG
    date = datetime.today().strftime('%Y-%m-%d')
    bucket_stg = 'input-data-stg-126916963195'
    file_stg = file.split(".")[0]
    file_stg = f'{file_stg}-{date}.parquet'
    path_stg = f's3://{bucket_stg}/{file_stg}'
    df.to_parquet(path_stg)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


