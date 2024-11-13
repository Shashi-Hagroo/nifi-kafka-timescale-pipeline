from kafka import KafkaConsumer
import json
from datetime import datetime
import psycopg2
import re

# Database and Kafka configuration
DB_NAME = "tsdb"
DB_USER = "tsdbadmin"
DB_PASSWORD = "l12zefbv2bo54ntl"
DB_HOST = "sagnuggvk0.bhhtile833.tsdb.cloud.timescale.com"
DB_PORT = "34267"
DB_SSLMODE = "require"

KAFKA_TOPIC_JSON = 'tracking_data_json'
KAFKA_TOPIC_CSV = 'tracking_data_csv'
KAFKA_SERVER = 'localhost:9092'

# Alarm code mapping - simplified example
alarm_mapping = {
    (0, 0): "0 Unknown",
    (108, 1): "1 Minor",
    (109, 2): "2 Moderate",
    (110, 3): "3 Serious",
    (111, 4): "4 Severe",
    (112, 5): "5 Critical"
}


def connect_db():
    conn = psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sslmode=DB_SSLMODE
    )
    create_table_if_not_exists(conn)
    return conn


def create_table_if_not_exists(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS vehicle_data (
            imei TEXT,
            timestamp TIMESTAMP,
            lon FLOAT,
            lat FLOAT,
            speed_mph FLOAT,
            alarm_code TEXT,
            alt FLOAT,
            odometer FLOAT,
            location TEXT,
            road_type TEXT
        )
    """)
    conn.commit()
    cursor.close()


# CSV transformation with field-by-field validation and error handling
def transform_company_x(data):
    try:
        # Remove unwanted JSON-like wrappers and split the CSV
        data = re.sub(r'[{}"]', '', data)  # Clean up quotes and braces
        fields = data.split(',')

        # Checking if CSV has at least the required number of fields
        if len(fields) < 8:
            print(f"Unexpected CSV format: {data}")
            return None

        imei, date_str, lon, lat, speed, _, alt, alarm_code = fields[:8]

        # Parsing fields individually with defaults and conversions
        timestamp = None
        if date_str and len(date_str) == 14:
            try:
                timestamp = datetime.strptime(date_str, "%Y%m%d%H%M%S").strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Invalid date format in CSV: {date_str}")

        lon = float(lon) if lon else None
        lat = float(lat) if lat else None
        speed_mph = float(speed) * 0.621371 if speed else 0.0
        alt = float(alt) if alt else None
        alarm_code_desc = alarm_mapping.get((0, int(alarm_code)), "0 Unknown")

        print(
            f"Parsed CSV - IMEI: {imei}, Timestamp: {timestamp}, Lon: {lon}, Lat: {lat}, Speed: {speed_mph}, Alt: {alt}, Alarm Code: {alarm_code_desc}")

        return {
            "imei": imei,
            "timestamp": timestamp,
            "lon": lon,
            "lat": lat,
            "speed_mph": speed_mph,
            "alarm_code": alarm_code_desc,
            "alt": alt
        }
    except (ValueError, IndexError) as e:
        print(f"Error processing CSV data {data}: {e}")
        return None


# JSON transformation with field-by-field validation and error handling
def transform_company_y(data):
    try:
        imei = data.get("imei")
        gps_time = data.get("gps_time")

        timestamp = None
        if gps_time and str(gps_time).isdigit():
            try:
                timestamp = datetime.utcfromtimestamp(int(gps_time)).strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                print(f"Invalid gps_time format: {gps_time}")
                return None
        else:
            print(f"Invalid gps_time format: {gps_time}")
            return None

        lon = float(data.get("lng", 0.0))
        lat = float(data.get("lat", 0.0))
        speed_mph = float(data.get("speed", 0)) * 0.621371
        alarm_code = int(data.get("alarmcode", 0))
        alarm_code_desc = alarm_mapping.get((0, alarm_code), "0 Unknown")
        odometer = float(data.get("Odometer", 0.0))
        location = data.get("LocationDesc", "")
        road_type = data.get("RoadTypeDescription", "")

        print(
            f"Parsed JSON - IMEI: {imei}, Timestamp: {timestamp}, Lon: {lon}, Lat: {lat}, Speed: {speed_mph}, Odometer: {odometer}, Location: {location}, Road Type: {road_type}, Alarm Code: {alarm_code_desc}")

        return {
            "imei": imei,
            "timestamp": timestamp,
            "lon": lon,
            "lat": lat,
            "speed_mph": speed_mph,
            "alarm_code": alarm_code_desc,
            "odometer": odometer,
            "location": location,
            "road_type": road_type
        }
    except (TypeError, ValueError) as e:
        print(f"Error processing JSON data {data}: {e}")
        return None


# Consumer setup and main processing function
consumer = KafkaConsumer(
    KAFKA_TOPIC_JSON,
    KAFKA_TOPIC_CSV,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    group_id='vehicle_data_group'
)


def insert_to_db(conn, record):
    if record is None:
        return
    cursor = conn.cursor()
    try:
        print(f"Inserting: {record}")
        cursor.execute("""
            INSERT INTO vehicle_data (imei, timestamp, lon, lat, speed_mph, alarm_code, alt, odometer, location, road_type)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            record['imei'],
            record['timestamp'],
            record['lon'],
            record['lat'],
            record['speed_mph'],
            record['alarm_code'],
            record.get('alt'),
            record.get('odometer'),
            record.get('location'),
            record.get('road_type')
        ))
        conn.commit()
    except psycopg2.Error as e:
        conn.rollback()
        print(f"Database insert error: {e}")
    finally:
        cursor.close()


def main():
    conn = connect_db()
    print("Connected to database, consuming messages from Kafka...")

    for message in consumer:
        try:
            data = message.value.decode('utf-8').strip()
            if not data:
                print("Skipping empty message")
                continue

            print(f"Received message: {data}")

            if data.startswith('{'):
                parsed_data = json.loads(data)
                print(f"Parsed JSON data: {parsed_data}")
                record = transform_company_y(parsed_data)
            else:
                print(f"CSV data: {data}")
                record = transform_company_x(data)

            if record:
                insert_to_db(conn, record)
                print(f"Inserted record for IMEI {record['imei']} at {record['timestamp']}")
            else:
                print("Skipping record due to parsing error")

        except Exception as e:
            print(f"Error processing message: {e}")

    conn.close()


if __name__ == "__main__":
    main()






