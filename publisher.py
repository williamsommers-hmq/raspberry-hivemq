#!/usr/bin/env python3

import asyncio
import json
from datetime import datetime
import random

from gmqtt import Client as MQTTClient

# Public MQTT broker for testing
BROKER_HOST = 'broker.hivemq.com'
BROKER_PORT = 1883
TOPIC = 'site1/line2/mixer3/motor1/sensors/mainmotor'

async def publish_data():
    client = MQTTClient("publisher-client")
    await client.connect(BROKER_HOST, BROKER_PORT)

    while True:
        data = {
            "datetime_stamp": datetime.now().isoformat(),
            "operation_time": random.uniform(100.0, 500.0),
            "voltage": random.uniform(220.0, 240.0),
            "current": random.uniform(5.0, 7.5),
            "last_maintenance": "2025-08-01"
        }
        payload = json.dumps(data)
        print(f"Publishing: {payload}")
        client.publish(TOPIC, payload, qos=1)
        await asyncio.sleep(5)

if __name__ == '__main__':
    print("Starting MQTT Publisher. Press Ctrl+C to exit.")
    try:
        asyncio.run(publish_data())
    except KeyboardInterrupt:
        print("Publisher stopped.")
