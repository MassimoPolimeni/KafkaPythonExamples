import json
import requests

# from confluent_kafka import avro, Consumer, Producer
# from confluent_kafka.avro import AvroConsumer, AvroProducer, CachedSchemaRegistryClient
# from faker import Faker


REST_PROXY_URL = "http://localhost:8082"


def produce():
    """Produces data using REST Proxy"""

    # Set the appropriate headers
    # See: https://docs.confluent.io/current/kafka-rest/api.html#content-types
    headers = {
        "Content-Type": "application/json"
    }
    # Define the JSON Payload to be sent to REST Proxy
    # To create data, use `asdict(ClickEvent())`
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    data = {"id": 123456, "message": "This is a message!"}

    # What URL should be used?
    # See: https://docs.confluent.io/current/kafka-rest/api.html#post--topics-(string-topic_name)
    resp = requests.post(
        f"{REST_PROXY_URL}/topics/test", data=json.dumps(data), headers=headers
    )

    try:
        resp.raise_for_status()
    except:
        print(f"Failed to send data to REST Proxy {json.dumps(resp.json(), indent=2)}")

    print(f"Sent data to REST Proxy {json.dumps(resp.json(), indent=2)}")



def main():
    """Runs the simulation against REST Proxy"""
    try:
        while True:
            produce()
    except KeyboardInterrupt as e:
        print("shutting down")
