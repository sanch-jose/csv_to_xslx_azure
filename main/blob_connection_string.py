from pyspark.sql import SparkSession
from io import BytesIO
from azure.storage.blob import BlobServiceClient
import pandas as pd
import os
import json

def cloud_event(req: func.HttpRequest):
    try:
        # Obtén el cuerpo del HTTP request en formato JSON
        event_data = req.get_json()

        # Devuelve el JSON en bruto como respuesta
        return func.HttpResponse(json.dumps(event_data), status_code=200, mimetype="application/json")

    except Exception as e:
        return func.HttpResponse(f"Ocurrió un error en el proceso: {str(e)}", status_code=500)



def cloud_event_parser(cloud_event):
        
    # Analiza el evento JSON
    event = json.loads(cloud_event)
    environment = event["cloudEvent"]["environment"]

     # Inicializa listas para recopilar los valores
    paths = []
    file_names = []
    containers = []

    results = event["cloudEvent"]["data"]["project"]["results"]
    
    for result in results:
        path = result["path"][1:]
        file_name = result["fileName"]
        paths.append(path)
        file_names.append(file_name)
        container = result["message"].split("/")[2]
        containers.append(container)

    if environment == "dev":
        connection_string = os.getenv("DEV")
    elif environment == "pre":
        connection_string = os.getenv("PRE")
    else:
        connection_string = os.getenv("PRO")

    return connection_string, containers, paths, file_names

def convert_csv_to_xlsx(cloud_event):
    # Set Spark session
    spark = SparkSession.builder.appName("ConvertCSVtoXLSX").getOrCreate()

    connection_string, containers, paths, file_names = cloud_event_parser(cloud_event)

    try:
        for path, file_name, container in zip(paths, file_names, containers):
            # Create blob's client and download the CSV
            blob_service_client = BlobServiceClient.from_connection_string(connection_string)
            container_client = blob_service_client.get_container_client(container)
            blob_client = container_client.get_blob_client(path + "/" + file_name)

            with BytesIO() as input_blob:
                blob_client.download_blob().download_to_stream(input_blob)
                input_blob.seek(0)
                df = pd.read_csv(input_blob, sep=';', quoting=3)
                df = df.map(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)

            # Changing the extension of the file to .xlsx
            base_blob_name, _ = os.path.splitext(path + "/" + file_name)
            xlsx_blob_name = base_blob_name + ".xlsx"

            # Converting DF to XLSX
            with BytesIO() as output_blob:
                df.to_excel(output_blob, index=False)
                output_blob.seek(0)

                # Uploading the XLSX file to the blob
                blob_client_xlsx = container_client.get_blob_client(xlsx_blob_name)
                blob_client_xlsx.upload_blob(output_blob.read(), overwrite=True)

            print(f"Conversión y subida completadas. Archivo XLSX: {xlsx_blob_name}")
    except Exception as e:
        print("Ocurrió un error en el proceso:", str(e))
    finally:
        # Stop Spark session
        spark.stop()
