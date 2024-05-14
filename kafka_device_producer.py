from confluent_kafka import Producer, KafkaError,Consumer
from datetime import datetime
from faker import Faker
import pandas as pd
import random
import json
import time

# Initialize Faker
f = Faker()
try:
    PATH=r"C:\Users\Alok mishra\Desktop\sql_project\ecom-analytics\data/patient.json"
    df = pd.read_json(PATH, typ="series")
    df = df.to_frame(name='key').reset_index(drop=True)

    keys = df['key'].apply(lambda x: x['key'])

    extracted_keys = keys.str.slice(3, 12)
except Exception as e:
    print("Error occured :  {e}")


conf = {
      # Update with your Confluent Kafka password
}
producer = Producer(conf)


sensor_types = ["EEG", "EKG", "Fetal_Monitor", "Hms", "ipm", "CGM", "CRRT", "CPAP", "pdm", "ctm", "cosm",
                 "bga", "icm", "vsm", "com", "rrm", "iip", "ote"]
timestamp_iso = datetime.now().isoformat()

def generate_device_reading():
    device_id = f.random_int(min=1, max=100)
    sensor_type = random.choice(sensor_types)
    timestamp = int(time.time())  # Current Unix timestamp
    return {
        "device_id": device_id,
        "sensor_type": sensor_type,
        "reading_value": f.random_int(min=50, max=150),
        "patient_id":f.random.choice(extracted_keys),
        "timestamp": timestamp_iso
   
    }
try:
   
    while True :
        device_reading = generate_device_reading()
        # Convert device reading to JSON
        device_reading_json = json.dumps(device_reading)
        # Produce message to Confluent Kafka topic with key as device_id
        producer.produce(topic='topic_0', key=str(device_reading['device_id']), value=device_reading_json)
        print(f"Produced message with key {device_reading['device_id']}: {device_reading_json}")
        producer.flush()
        

except Exception as e:
    print(f"Error occurred: {e}")
    

