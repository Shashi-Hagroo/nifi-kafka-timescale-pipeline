Data Processing Pipeline with Apache NiFi, Kafka, and TimescaleDB

This repository contains a data processing pipeline that ingests, processes, and stores data from two different companies (Company X and Company Y) using Apache NiFi, Kafka, Python, and TimescaleDB. The pipeline handles JSON and CSV data ingestion, applies specific transformations, and loads the data into a TimescaleDB database.

Please note that there is an error in the Nifi flow processing of data that has resulted in a loss of csv data, which presents itself as null values. The troubleshooting of this problem was suspended to complete the supporting documentation for this project. 

Overview

The pipeline processes incoming data from Company X (CSV format) and Company Y (JSON format) by:

    Ingesting files with NiFi
    Routing and converting each data type to a consistent format
    Publishing data to Kafka topics
    Transforming and ingesting data into TimescaleDB via Python

Prerequisites

    Apache NiFi installed and configured
    Apache Kafka set up with topics for each data type
    TimescaleDB set up within PostgreSQL
    Python 3.x installed with necessary libraries (listed in requirements.txt)

Architecture

(Please see included MIC3 Data Pipeline Architecture PNG file)

    Data Ingestion (Apache NiFi): NiFi GetFile and RouteOnAttribute processors identify file types and process CSV and JSON data separately.
    Data Processing (NiFi ConvertRecord): Data is converted to a consistent JSON format using ConvertRecord.
    Publishing to Kafka: Data is sent to Kafka topics tracking_data_csv and tracking_data_json.
    Data Transformation (Python): Python consumers ingest data from Kafka, apply transformations, and store it in TimescaleDB.

Setup
Step 1: Configure NiFi Flow

    GetFile Processors:
        Configure two GetFile processors to monitor directories for Company X (CSV) and Company Y (JSON) files.

    RouteOnAttribute Processor:
        Add a RouteOnAttribute processor to separate data based on file type:
            Route .csv files to a ConvertRecord processor for CSV data.
            Route .json files to a ConvertRecord processor for JSON data.

    ConvertRecord Processors:
        Configure ConvertRecord processors to handle specific schemas:
            CSV Processor: Use CSVReader and set the schema to Company X's data structure.
            JSON Processor: Use JsonRecordSetWriter to validate and process Company Y’s data structure.

    PublishKafka Processors:
        Publish the transformed data to Kafka topics:
            Company X data to tracking_data_csv.
            Company Y data to tracking_data_json.

Step 2: Configure Kafka

    Ensure Kafka is running and create two topics:
        tracking_data_csv
        tracking_data_json
    Make sure each NiFi PublishKafka processor points to the correct topic.

Step 3: Configure TimescaleDB

    Database Setup:
        Create a TimescaleDB database and tables to store tracking data.
        Define tables with the necessary fields for each company’s data schema.

    Schema Example:
CREATE TABLE tracking_data (
    imei TEXT,
    gps_time TIMESTAMPTZ,
    server_time TIMESTAMPTZ,
    lng DOUBLE PRECISION,
    lat DOUBLE PRECISION,
    speed DOUBLE PRECISION,
    alarmcode INTEGER,
    odometer DOUBLE PRECISION,
    locationdesc TEXT,
    roadtypedescription TEXT
);

    Indexes:
        Set indexes for optimized querying on frequently queried columns like imei and gps_time.

Step 4: Run Python Kafka Consumers

1. Install required libraries:
pip install -r requirements.txt

2. Run the Python script to consume data:
Task_2.py

The script will:

    Connect to Kafka, read data from each topic, and process records.
    Apply transformations such as datetime parsing, alarm code mapping, and speed conversions.
    Insert transformed data into TimescaleDB.

Data Transformation and Schema Mapping

    Alarm Code Mapping: Alarm codes are mapped to descriptions.
    Speed Conversion: Speed is converted if needed to a standardized format.
    Timestamp Parsing: Date and time fields are parsed and converted into TIMESTAMPTZ format.

Running the Pipeline

    Start NiFi and ensure the flow is activated.
    Run Kafka and verify topics are available.
    Start TimescaleDB and confirm tables are ready.
    Run the Python consumer script:
        Data should flow through NiFi to Kafka and then be consumed and stored in TimescaleDB.

