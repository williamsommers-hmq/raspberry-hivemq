import asyncio
import json
from datetime import datetime
import random
import signal

from gmqtt import Client as MQTTClient

# Public MQTT broker for testing
BROKER_HOST = 'broker.hivemq.com'
BROKER_PORT = 1883
TOPIC = 'plant1/line2/mixer3/primary_motor'

STOP = asyncio.Event()

def on_connect(client, flags, rc, properties):
    if rc == 0:
        print('Connected to broker.')
    else:
        print(f'Failed to connect to broker with reason code: {rc}')

def on_disconnect(client, packet, exc=None):
    print('Disconnected from broker. Reconnecting...')

def ask_exit(*args):
    STOP.set()

async def main():
    client = MQTTClient("robust-publisher-client")

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect

    # Set reconnect_retries to a large number for persistent reconnection attempts
    client.reconnect_retries = 100
    client.reconnect_delay = 5  # seconds

    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), ask_exit)

    await client.connect(BROKER_HOST, BROKER_PORT)

    while not STOP.is_set():
        data = {
            "datetime_stamp": datetime.now().isoformat(),
            "operation_time": random.uniform(100.0, 500.0),
            "voltage": random.uniform(220.0, 240.0),
            "current": random.uniform(5.0, 7.5),
            "last_maintenance": "2025-08-01"
        }
        payload = json.dumps(data)
        
        try:
            if client.is_connected:
                print(f"Publishing: {payload}")
                client.publish(TOPIC, payload, qos=1)
            else:
                print("Client is not connected. Waiting for reconnection...")
        except Exception as e:
            print(f"An error occurred during publishing: {e}")

        await asyncio.sleep(5)

    await client.disconnect()
    print("Publisher stopped.")


if __name__ == '__main__':
    print("Starting Robust MQTT Publisher. Press Ctrl+C to exit.")
    asyncio.run(main())
