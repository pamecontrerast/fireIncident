import json
import pandas as pd 

def lambda_handler(event, context):
    # TODO implement
    
    bucket = event['Records'][0]['s3']['bucket']['name']
    file = event['Records'][0]['s3']['object']['key']
    path = f's3://{bucket}/{file}'
    df = pd.read_parquet(path)
    
    bucket_dwh = 'datawarehouse-bi-126916963195'
    
    # dim_battalion
    file_dim_battalion = 'dim_battalion.parquet'
    path_dwh_battalion = f's3://{bucket_dwh}/{file_dim_battalion}'
    #se crea archivo vacio, simil a crear tabla inicial en bbdd
    pd.DataFrame([], columns=['id_battalion', 'battalion', 'station_area', 'box']).to_parquet(path_dwh_battalion)
    
    fact_incident = assign_battalion(df, path_dwh_battalion)
    
        
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }

    
def assign_battalion(df, path_dwh_battalion):
    df_tbl_dim_battalion = pd.read_parquet(path_dwh_battalion)
    #se intenta asignar batallon
    df = df.merge(df_tbl_dim_battalion, how="left", on=['battalion','station_area','box'])
    df_sin_batallon = df[df.id_battalion.isna()]#[["battalion", "station_area", "box"]]
    if(len(df_sin_batallon)>0):
        df = df.drop(columns=["id_battalion"])
        df_tbl_dim_battalion_sin_id = df_sin_batallon[["battalion", "station_area", "box"]].drop_duplicates()
        df_tbl_dim_battalion_sin_id = df_tbl_dim_battalion_sin_id.reset_index(drop=True).reset_index().rename(columns={'index':'id_battalion'})
        #se mueve segun el largo del batallon guardado
        df_tbl_dim_battalion_sin_id.id_battalion = df_tbl_dim_battalion_sin_id.id_battalion + len(df_tbl_dim_battalion)
        df_tbl_dim_battalion = pd.concat([df_tbl_dim_battalion, df_tbl_dim_battalion_sin_id])
        df = df.merge(df_tbl_dim_battalion, how="left", on=['battalion','station_area','box'])
        df_tbl_dim_battalion.to_parquet(path_dwh_battalion)
        df = df.drop(columns=['battalion','station_area','box'])
    return df 
