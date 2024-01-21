from kafka import KafkaConsumer
import json
import requests
import time
import calendar

interval = 30
buffer_array = {}
current_time = time.time()
kafka_bootstrap_servers = ['localhost:9092']
kafka_topic = 'task5-prsh'


def sendToDatabase():
    query = "INSERT INTO TASK_5_PRSh (data,fromts,tots) VALUES" + "('" + json.dumps(buffer_array) + "'," + str(
        int(current_time * 1000000)) + "," + str(int((current_time + interval) * 1000000)) + ")"
    r = requests.get("http://localhost:9000/exec?query=" + query)
    print(r.status_code, query)
    buffer_array.clear()


def addInfoAboutData(itm):
    if itm in buffer_array:
        buffer_array[itm] += 1
    else:
        buffer_array[itm] = 1


class obj:
    def __init__(self, dict1):
        self.__dict__.update(dict1)


def dict2obj(dict1):
    return json.loads(json.dumps(dict1), object_hook=obj)


consumer = KafkaConsumer(
    kafka_topic,
    bootstrap_servers=kafka_bootstrap_servers,
    value_deserializer=lambda x: json.loads(x.decode(('utf-8')))
)
for message in consumer:
    datalist = dict2obj(message.value)
    data = datalist.data
    timestamp = datalist.timestamp
    print(data)
    if current_time + interval < timestamp:
        sendToDatabase()
        current_time = current_time + interval
    addInfoAboutData(data)
