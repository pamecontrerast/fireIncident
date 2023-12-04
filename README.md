# Descripción de la solución 

## Diagrama de la solución en AWS

La solución se implementó en AWS, usando los servicios S3, Lambda y Cloudwatch y se desarrolló en Python.

![aws](/img/Diagrama-AWS.png)

La periodicidad de la carga se fijó como diaria, esto se configuró de acuerdo a la detección de la carga del archivo en S3, se activa un trigger que detecta archivos nuevos en una ruta definida para este archivo y se ejecuta el proceso de carga. 

![trigger](/img/triggerS3.png)

El diagrama se muestra el DWH como un bucket se S3, por temas de costos y la configuración de una base de datos, se simuló el manejo de las tabla de hechos y dimensiones de una base de datos, como archivos.

Este diagrama se resume en tres capas:

![s3](/img/buckets.png)

- Capa Raw: se carga el archivo en bruto a S3.
- Capa STG: se da formato a los campos que se consideren necesarios. Para este caso se formateó la incident_date a datetime y se forzó el tipo de datos de algunos campos.

- Capa Final: se encuentra la solución para este caso específico. Para este caso se genera la tabla de hechos y sus dimensiones.


## Diagrama DWH

Como se mencionó anteriormente, el modelo del DWH se simuló en S3 mediante archivos, pero si diseñó el posible modelo para este solución, para ser implementado en una base de datos de acuerdo a la necesidad, Redshift es la indicada si es data para análisis.

![DWH](/img/fact-dim.png)

La tabla "fact_incident" puede o no tener más datos de la sábana de datos entregados, pero para este caso no se consideraron más campos adicionales, se entiende que dependiendo de la necesidad a analizar son los datos que se van a cargar en el modelo, los datos no cargados permanenen en la capa Raw para otro posible uso.


Para todas las dimensiones se deben considerar los posibles nuevos registros que no van a cruzar al momento de insertar la data, para esto hay que considerar un upsert de los datos.

## Observaciones finales

Principalmente por temas de tiempo, no se implementaron algunas soluciones como:

- Versionamiento y desarrollo en serverless: el desarrollo se aplicó directamente en la consola de AWS, sin embargo se incluye en la entrega los códigos .py de las lambdas.

- Upsert: sólo se escribió código para posibles nuevos registros en "dim_battalion".

Ejemplo del upsert: esta lógica debería ser aplicada en todas las dimensiones.
```bash
def assign_battalion(df):
    df_tbl_dim_battalion = pd.read_parquet("dim_battalion.parquet")
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
        df_tbl_dim_battalion.to_parquet("dim_battalion.parquet")
        df = df.drop(columns=['battalion','station_area','box'])
    return df 
```

## ¿Qué contiene el repositorio?

En "lambda" se encuentran los codigos de las lambdas que se implementaron en cada capa.

El archivo  "analisis-data.ipynb" contiene algunos análisis que realicé en el desarrollo de estos pasos.

