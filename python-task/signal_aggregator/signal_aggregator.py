from copy import deepcopy
import json
import time
import statistics
import paho.mqtt.client as mqtt
import psycopg2
import sys
import threading
import logging

logging.basicConfig(format='%(asctime)s %(message)s' , datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
logging.warning('This event was logged.')

MQTT_BROKER = "mqtt"
MQTT_PORT = 1883
MQTT_TOPIC_MEASUREMENTS = "measurements"
MQTT_TOPIC_IGNORED = "ignored"

DB_HOST = "db"
DB_PORT = "5432"
DB_NAME = "measurements_data"
DB_USER = "postgres"
DB_PASSWORD = "password"


T = int(sys.argv[1])  
N = int(sys.argv[2])  

measurements = []
ignored = []
is_aggregating = False
lock = threading.Lock()

def dump_to_db(period_start: str, period_end: str, n: int, min_val: float, max_val: float, median_val: float, average_val: float) -> None:
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
                INSERT INTO measurements_aggregation (period_start, period_end, n, min, max, median, average)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, (period_start, period_end, n, min_val, max_val, median_val, average_val))
    conn.commit()
    cur.close()
    conn.close()

def init_db() -> None:
    # Połączenie z bazą danych
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS measurements_aggregation (
            period_start TIMESTAMP,
            period_end TIMESTAMP,
            n INTEGER,
            min FLOAT,
            max FLOAT,
            median FLOAT,
            average FLOAT
        )
    """)
    conn.commit()
    conn.close()

def init_mqtt() -> None:
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.subscribe(MQTT_TOPIC_MEASUREMENTS)
    client.message_callback_add(MQTT_TOPIC_MEASUREMENTS, update_measurements)
    client.loop_start()

def save_aggregation() -> None:
    global measurements, is_aggregating
    with lock:
        arr = deepcopy(measurements)
        if len(arr) < N:
          return
        elif len(arr) >= N:
            is_aggregating = True
            periods = [m['time'] for m in arr]
            period_start = min(periods)
            period_end = max(periods)
            n = len(arr)
            values = [m["value"] for m in arr]
            min_val = min(values)
            max_val = max(values)
            median_val = statistics.median(values)
            average_val = sum(values)/ len(values)
            dump_to_db(period_start, period_end, n, min_val, max_val, median_val, average_val)
            measurements.clear()

def update_measurements(client, userdata, msg):
    global measurements, ignored, is_aggregating
    data = json.loads(msg.payload.decode("utf-8"))
    with lock:
        if not is_aggregating:
            measurements.append(data)
        else:
            client.publish(MQTT_TOPIC_IGNORED, msg.payload.decode("utf-8"))

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logging.info("Connected successfully")
    else: 
        logging.info("Connection eror code [{rc}]")

def on_disconnect(client, userdata, rc):
    logging.info(f"Disconnected, code [{rc}]")

def run():
    global is_aggregating
    while True:
        save_aggregation()
        time.sleep(T)
        is_aggregating = False

if __name__ == '__main__':
    init_db()
    init_mqtt()
    run()
    
