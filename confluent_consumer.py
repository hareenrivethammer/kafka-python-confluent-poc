from confluent_kafka import Consumer


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

config = read_ccloud_config('client.properties')
config['group.id'] = 'python-consumer'
consumer=Consumer(config)

print('Kafka Consumer has been initiated...')


consumer.subscribe(['Test'])



try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:

            print("Waiting...")
        elif msg.error():
            print("ERROR: %s".format(msg.error()))
        else:
            print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))

except KeyboardInterrupt:
    pass
finally:
    # Leave group and commit final offsets
    consumer.close()
