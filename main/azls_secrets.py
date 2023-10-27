from azure.identity import DefaultAzureCredential, ClientSecretCredential
from pyspark.sql import SparkSession
from io import BytesIO
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
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
        
    # Analyses and parses the Cloud Event
    event = json.loads(cloud_event)
    environment = event["cloudEvent"]["environment"]

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
        client_id = os.getenv("CLIENT_ID_DEV")
        client_secret = os.getenv("CLIENT_SECRET_DEV")
        client_tenant_id = os.getenv("CLIENT_TENANT_ID_DEV")
        storage_account = os.getenv("STORAGE_ACCOUNT_DEV")
    elif environment == "pre":
        client_id = os.getenv("CLIENT_ID_PRE")
        client_secret = os.getenv("CLIENT_SECRET_PRE")
        client_tenant_id = os.getenv("CLIENT_TENANT_ID_PRE")
        storage_account = os.getenv("STORAGE_ACCOUNT_PRE")
    else:
        client_id = os.getenv("CLIENT_ID_PRO")
        client_secret = os.getenv("CLIENT_SECRET_PRO")
        client_tenant_id = os.getenv("CLIENT_TENANT_ID_PRO")
        storage_account = os.getenv("STORAGE_ACCOUNT_PRO")

    return client_id, client_secret, client_tenant_id, storage_account, containers, paths, file_names

def convert_csv_to_xlsx(cloud_event):
    
    spark = SparkSession.builder.appName("ConvertCSVtoXLSX").getOrCreate()

    client_id, client_secret, client_tenant_id, storage_account, containers, paths, file_names = cloud_event_parser(cloud_event)
    credential = ClientSecretCredential(client_tenant_id, client_id, client_secret)

    try:
        # Creates a blob client to access the files
        blob_service_client = BlobServiceClient(account_url=f"https://{storage_account}.blob.core.windows.net", credential=credential)

        for path, file_name, container in zip(paths, file_names, containers):
            container_client = blob_service_client.get_container_client(container)
            blob_client = container_client.get_blob_client(f"{path}/{file_name}")

            # Downloads the CSV file
            download_stream = blob_client.download_blob()
            content = download_stream.readall()

            # Loading the CSV in a Pandas DF
            df = pd.read_csv(BytesIO(content), sep=';', quoting=3, dtype='object')

            # Processing the DF to avoid issued when transforming to XLSX
            df = df.map(lambda x: x.encode('unicode_escape').decode('utf-8') if isinstance(x, str) else x)

            # Changing file extension to .xlsx
            base_blob_name, _ = os.path.splitext(f"{path}/{file_name}")
            xlsx_blob_name = f"{base_blob_name}.xlsx"

            # Converting the DDF to XLSX
            with BytesIO() as output_blob:
                df.to_excel(output_blob, index=False)
                output_blob.seek(0)

                # Uploading the XLSX to the blob
                blob_client_xlsx = container_client.get_blob_client(xlsx_blob_name)
                blob_client_xlsx.upload_blob(output_blob.read(), overwrite=True)

            print(f"Conversión y subida completadas. Archivo XLSX: {xlsx_blob_name}")
    except Exception as e:
        print("Ocurrió un error en el proceso:", str(e))
    finally:
        # Stopping Spark session
        spark.stop()
