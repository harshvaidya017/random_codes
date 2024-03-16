from kafka import KafkaConsumer
import json
import csv
from avro import schema, datafile, io
from avro.io import DatumReader
from avro.datafile import DataFileReader
from io import BytesIO  # Import BytesIO for Avro message processing

# Define Kafka consumers for different types of data
consumer_json = KafkaConsumer('ad_impressions_topic',
                              bootstrap_servers=['localhost:9092'],
                              value_deserializer=lambda m: json.loads(m.decode('utf-8')))

def process_csv_message(message):
    # Placeholder function for processing CSV messages
    pass

consumer_csv = KafkaConsumer('clicks_conversions_topic',
                             bootstrap_servers=['localhost:9092'],
                             value_deserializer=lambda m: process_csv_message(m.decode('utf-8')))

def process_avro_message(message):
    # Placeholder function for processing Avro messages
    pass

consumer_avro = KafkaConsumer('bid_requests_topic',
                               bootstrap_servers=['localhost:9092'],
                               value_deserializer=lambda m: process_avro_message(m.decode('utf-8')))

# Function to process JSON messages (ad impressions)
def process_json_message(message):
    try:
        # Assuming the JSON message contains information about ad impressions
        ad_data = message  # For simplicity, let's assume the entire message is the ad data
        # Further processing steps could involve validation, transformation, enrichment, etc.
        # Example: Extracting relevant fields from the JSON message
        ad_id = ad_data['ad_id']
        user_id = ad_data['user_id']
        timestamp = ad_data['timestamp']
        website = ad_data['website']
        
        # Example: Performing some processing logic
        processed_data = {
            'ad_id': ad_id,
            'user_id': user_id,
            'timestamp': timestamp,
            'website': website,
            # Add more processed fields as needed
        }
        
        # Example: Writing processed data to a log file
        with open('processed_data_log.txt', 'a') as log_file:
            log_file.write(json.dumps(processed_data) + '\n')
    except Exception as e:
        print(f"Error processing JSON message: {str(e)}")

# Placeholder function to process CSV messages
def process_csv_message(message):
    pass

# Placeholder function to process Avro messages
def process_avro_message(message):
    pass

# Iterate over Kafka messages and process them
for message in consumer_json:
    process_json_message(message.value)

for message in consumer_csv:
    process_csv_message(message.value)

for message in consumer_avro:
    process_avro_message(message.value)

# Function to store processed data in a file
def store_to_file(data):
    try:
        with open('processed_data.json', 'a') as json_file:
            # Write the processed data to the file
            json.dump(data, json_file)
            json_file.write('\n')
    except Exception as e:
        print(f"Error storing data to file: {str(e)}")

# Function to store processed data in a database
def store_to_database(data):
    try:
        # Connect to the database
        # Example: Assuming SQLite database
        import sqlite3
        conn = sqlite3.connect('advertising.db')
        cursor = conn.cursor()

        # Store data in the database
        # Example: Assuming a table named 'processed_data'
        query = "INSERT INTO processed_data (ad_id, user_id, timestamp, website) VALUES (?, ?, ?, ?)"
        cursor.execute(query, (data['ad_id'], data['user_id'], data['timestamp'], data['website']))
        conn.commit()

        # Close the database connection
        conn.close()
    except Exception as e:
        print(f"Error storing data to database: {str(e)}")

# Store processed data in appropriate storage
for message in consumer_json:
    store_to_file(message.value)

for message in consumer_csv:
    store_to_database(message.value)

for message in consumer_avro:
    store_to_database(message.value)
