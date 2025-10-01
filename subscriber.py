#!/usr/bin/env python3

import asyncio
import json
import signal

from gmqtt import Client as MQTTClient

# Public MQTT broker for testing
BROKER_HOST = 'broker.hivemq.com'
BROKER_PORT = 1883
TOPIC = 'site1/line2/mixer3/motor1/sensors/mainmotor'

STOP = asyncio.Event()

def on_connect(client, flags, rc, properties):
    print(f'Connected to broker with result code: {rc}')
    client.subscribe(TOPIC, qos=1)

def on_message(client, topic, payload, qos, properties):
    print(f'Received message on topic "{topic}"')
    try:
        data = json.loads(payload)
        print(json.dumps(data, indent=2))
    except json.JSONDecodeError:
        print(f"Error decoding JSON: {payload.decode()}")

def on_disconnect(client, packet, exc=None):
    print('Disconnected from broker.')

def ask_exit(*args):
    STOP.set()

async def main():
    client = MQTTClient("subscriber-client")

    client.on_connect = on_connect
    client.on_message = on_message
    client.on_disconnect = on_disconnect

    loop = asyncio.get_event_loop()
    for signame in ('SIGINT', 'SIGTERM'):
        loop.add_signal_handler(getattr(signal, signame), ask_exit)

    await client.connect(BROKER_HOST, BROKER_PORT)

    print(f"Subscribed to topic: {TOPIC}")
    print("Listening for messages. Press Ctrl+C to exit.")
    await STOP.wait()
    await client.disconnect()

if __name__ == '__main__':
    asyncio.run(main())
