import json
import time
import calendar
from kafka import KafkaConsumer, KafkaProducer
import random

kafka_bootstrap_servers = ['localhost:9092']
kafka_topic = 'task5-prsh'


def consumerEvent():
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        value_deserializer=lambda x: json.loads(x.decode(('utf-8')))
    )
    print(consumer)
    for message in consumer:
        print(message)


def producerEvent():
    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap_servers,
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    for e in range(1000):
        name=generate_random_animal_name()
        data = {'id': e, 'data': name, 'timestamp': time.time()}
        print(data)
        producer.send(kafka_topic, value=data)
        time.sleep(1)


def generate_random_animal_name():
    animal_names = ["Lion", "Tiger", "Rabbit", "Fox", "Wolf", "Bear", "Elephant", "Giraffe", "Crocodile", "Monkey",
                    "Penguin", "Kangaroo", "Panda", "Koala", "Horse", "Llama", "Cat", "Dog", "Rabbit", "Hamster"]

    return random.choice(animal_names)

if __name__ == '__main__':
    producerEvent()
