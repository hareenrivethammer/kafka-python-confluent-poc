import json
import time
from uuid import uuid4
from confluent_kafka import Producer


def read_ccloud_config(config_file):
    conf = {}
    with open(config_file) as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                parameter, value = line.strip().split('=', 1)
                conf[parameter] = value.strip()
    print(conf)
    return conf


def delivery_report(errmsg, msg):
    if errmsg is not None:
        print("Delivery failed for Message: {} : {}".format(msg.key(), errmsg))
        return
    print('Message: {} successfully produced to Topic: {} Partition: [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))


producer = Producer(read_ccloud_config("client.properties"))
for i in range(22, 30):
    producer.produce("Test", key=str(uuid4()), value=json.dumps({"type":"User", "data":{"username":f"sp{i}","email":"sp017@gmail.com"}}), on_delivery=delivery_report)
    producer.poll(10000)
    producer.flush()
    # time.sleep(10)