import time
import json
import paho.mqtt.client as mqtt
import random
from datetime import datetime

MQTT_BROKER = "mqtt"
MQTT_PORT = 1883
MQTT_TOPIC = "measurements"

def publish_measurements(client):
    while True:
        measurement = {
            "time": datetime.now().strftime("%Y.%m.%d %H:%M:%S"),
            "value": round(random.uniform(1000.0, 3000.0), 2),
            "unit": "V"
        }
        time.sleep(3)
        client.publish(MQTT_TOPIC, json.dumps(measurement))
        

def main():
    client = mqtt.Client()
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()
    try:
        publish_measurements(client)
    except KeyboardInterrupt:
        client.loop_stop()
        client.disconnect()

if __name__ == "__main__":
    main()





