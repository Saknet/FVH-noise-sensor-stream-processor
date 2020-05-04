import json
import os
import sys

from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer
from ksql import KSQLAPI
from producer import produce_messages

client = KSQLAPI('https://kafka11.fvh.fi:8088/')
N_TOPIC = os.environ.get('test.sputhan.finest.cesva.v1.noise.sentilo.TA120-T246174-N')
O_TOPIC = os.environ.get('test.sputhan.finest.cesva.v1.noise.sentilo.TA120-T246174-O')
S_TOPIC = os.environ.get('test.sputhan.finest.cesva.v1.noise.sentilo.TA120-T246174-S')
U_TOPIC = os.environ.get('test.sputhan.finest.cesva.v1.noise.sentilo.TA120-T246174-U')

def basic_consume_loop(consumer, topics):
    running = True

    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                produce_messages(msg)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        running = False 

if __name__ == '__main__':

    consumer = Consumer(
        bootstrap_servers = '127.0.0.1:9092',
        key_deserializer = lambda item: json.loads(item.decode('utf-8')),
        value_deserializer = lambda item: json.loads(item.decode('utf-8')),
        auto_offset_reset = 'smallest'
    )

    topics = [N_TOPIC, O_TOPIC, S_TOPIC, U_TOPIC]

    basic_consume_loop(consumer, topics)
