from confluent_kafka import Producer

def acked(err, msg):
    if err is not None:
        print("Failed to deliver message: %s: %s" % (str(msg), str(err)))
    else:
        print("Message produced: %s" % (str(msg)))

conf = {'bootstrap.servers': "localhost:9092"}

producer = Producer(conf)
producer.produce("test", value="value", callback=acked)

# Wait up to 10 second for events. Callbacks will be invoked during
# this method call if the message is acknowledged.
producer.poll(10)
