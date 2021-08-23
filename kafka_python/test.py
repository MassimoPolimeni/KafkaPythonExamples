from kafka import KafkaProducer
import json


def on_send_success(record_metadata):
    print(record_metadata.topic)
    print(record_metadata.partition)
    print(record_metadata.offset)
    print(record_metadata)


def on_send_error(excp):
    print("I am an errback", exc_info=excp)


producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

producer.send("topic-test", {"key": "message"}).add_callback(
    on_send_success
).add_errback(on_send_error)
producer.flush()
