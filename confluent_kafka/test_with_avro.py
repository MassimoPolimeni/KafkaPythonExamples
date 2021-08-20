import json
import uuid

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
# from confluent_kafka.avro import SchemaRegistryClient, Schema
# from utils.load_avro_schema_from_file import load_avro_schema_from_file
# from utils.parse_command_line_args import parse_command_line_args


def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("\n\nTOPIC: %s \nMESSAGE: %s" % (msg.topic(), msg.value()))


def load_avro_schema_from_file(schema_file):
    key_schema_string = """
    {"type": "string"}
    """

    key_schema = avro.loads(key_schema_string)
    value_schema = avro.load("./avro/" + schema_file)

    return key_schema, value_schema


conf = {
    "bootstrap.servers": "localhost:9092",
    'schema.registry.url': 'http://schema-registry-host:8081'
}



key_schema, value_schema = load_avro_schema_from_file("topic_avro_schema.json")

producer_config = {
    "bootstrap.servers": "localhost:9092",
    "schema.registry.url": "http://schema-registry-host:8081"
}

producer = AvroProducer(producer_config, default_key_schema=key_schema, default_value_schema=value_schema)

try:
    producer.produce(topic="test", value="{'id':'123456','message':'this is a message'}")
except Exception as e:
    print("Exception while producing record value")
else:
    print(f"Successfully producing record value {e}")

producer.flush()





# avro_schema = Schema(schema_str, 'AVRO')
# sr = SchemaRegistryClient("http://schema-registry-host:8081")
# _schema_id = sr.register_schema("yourSubjectName", avro_schema)

# avroProducer = AvroProducer(conf, default_value_schema=avro_schema)

# avroProducer.produce(topic="test", value="message", callback=acked)

# avroProducer.poll(10)



# avroProducer = AvroProducer({
#     'bootstrap.servers': bootstrap_servers[0]+','+bootstrap_servers[1],
#     'schema.registry.url':'http://my_adress.com:8081',
#     },
#     default_value_schema=value_schema
#     )

# for i in range(0, 25000):
#     value = {"name":"Yuva","favorite_number":10,"favorite_color":"green","age":i*2}
#     avroProducer.produce(topic='my_topik14', value=value)
#     avroProducer.flush(0)
# print('Finished!')
