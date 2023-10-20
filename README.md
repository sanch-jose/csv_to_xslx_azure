# Introduction
This code is a useful utility for automating the conversion of CSV files to XLSX format and storing the converted files back in Azure Blob Storage. It leverages Azure services and other Python libraries to accomplish this task.

## Code Overview
The code consists of two main functions:

### cloud_event_parser(cloud_event)

This function parses a Cloud Event payload and extracts relevant information such as Azure credentials, container names, file paths, and file names.
It is intended to be used in conjunction with Azure Functions or other cloud-based event triggers to automate the conversion process.

### convert_csv_to_xlsx(cloud_event)

This function uses Apache Spark to convert CSV files to XLSX format and upload them back to Azure Blob Storage.
It first calls cloud_event_parser to extract necessary parameters and then performs the conversion and upload.

## Dependencies
The code depends on several Python libraries and Azure SDKs. Ensure you have these dependencies installed in your Python environment:

azure-identity: Provides authentication for Azure services.

pyspark.sql: Offers the SparkSession for processing CSV files.

io.BytesIO: Used for stream processing.

azure.storage.blob: Provides the Azure Blob Storage client for file operations.

pandas: Used for handling data in DataFrames.

os: Used for environment-specific credential retrieval.

json: Used for parsing the Cloud Event payload.

## Usage
Ensure that the required dependencies are installed. You can install them using pip:

pip install azure-identity pyspark pandas

Set up Azure Blob Storage and configure your environment variables:

For the environment variable, set either "dev," "pre," or "pro" to specify your environment.

### Connection string

Set up the connection strin for each environment: "dev", "pre" and "pro".

### ACLs
For each environment, set the following environment variables:

CLIENT_ID_DEV, CLIENT_SECRET_DEV, CLIENT_TENANT_ID_DEV, STORAGE_ACCOUNT_DEV (for the "dev" environment).

CLIENT_ID_PRE, CLIENT_SECRET_PRE, CLIENT_TENANT_ID_PRE, STORAGE_ACCOUNT_PRE (for the "pre" environment).

CLIENT_ID_PRO, CLIENT_SECRET_PRO, CLIENT_TENANT_ID_PRO, STORAGE_ACCOUNT_PRO (for the "pro" environment).

Implement a Cloud Event trigger (e.g., Azure Functions) to call the convert_csv_to_xlsx function with the Cloud Event payload as an argument. The payload should contain information about the files to be converted.
