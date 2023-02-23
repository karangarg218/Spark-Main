import pymongo
from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import pandas as pd
count = 0
from typing import List


API_KEY = '3DE33YMBIGNYT7DL'
ENDPOINT_SCHEMA_URL = 'https://psrc-ko92v.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'Al/PSF0fgjNf3TOan1rnOpJThbg52/fk24dQkSo3uUzJIkHEoZ/XM1xIHCGrFQuh'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'

SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'QZ4IELQGPAOCHVUH'
SCHEMA_REGISTRY_API_SECRET = 'hf/WuZYzMIPOFhwRAXn3T8DWaR//wopasz4m8uBa+jV3fjFNrH7CQOFFYGAZQGxJ'

def get_mongo(db,collection):
    client = pymongo.MongoClient("mongodb+srv://kafka:admin1234@kafka.u1y3wwk.mongodb.net/?retryWrites=true&w=majority")
    db=client[db]
    collect=db[collection]
    return  collect



class Time:
    def __init__(self, record: dict):
        for k, v in record.items():
            self.__setattr__(k, v)

        self.record = record

    @staticmethod
    def dict_to_Time(data: dict, ctx):
        return Time(record=data)

    def __str__(self):
        return f"{self.record}"

def sasl_conf():
    sasl_conf = {'sasl.mechanism': SSL_MACHENISM,
                 # Set to SASL_SSL to enable TLS support.
                 #  'security.protocol': 'SASL_PLAINTEXT'}
                 'bootstrap.servers': BOOTSTRAP_SERVER,
                 'security.protocol': SECURITY_PROTOCOL,
                 'sasl.username': API_KEY,
                 'sasl.password': API_SECRET_KEY
                 }
    return sasl_conf


def schema_config():
    return {'url': ENDPOINT_SCHEMA_URL,

            'basic.auth.user.info': f"{SCHEMA_REGISTRY_API_KEY}:{SCHEMA_REGISTRY_API_SECRET}"

            }



def delivery_report(err, msg):
    """
    Reports the success or failure of a message delivery.
    Args:
        err (KafkaError): The error that occurred on None on success.
        msg (Message): The message that was produced or failed.
    """

    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))

        return
    global count
    count = count + 1
    #print('User record {} successfully produced to {} [{}] at offset {}'.format(
    # msg.key(), msg.topic(), msg.partition(), msg.offset()))


def main(topic):
    records=[]
    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    # getting latest schema by name
    my_schema = schema_registry_client.get_latest_version("time-value")
    # sotring the schema in my_Str
    my_str = my_schema.schema.schema_str

    json_deserializer = JSONDeserializer(my_str,
                                         from_dict=Time.dict_to_Time)

    consumer_conf = sasl_conf()

    consumer_conf.update({
        'group.id': 'Time',
        'auto.offset.reset': "earliest"})

    consumer = Consumer(consumer_conf)

    consumer.subscribe([topic])

    collection=get_mongo(db="kafka",collection="Time")
    while True:
        try:
            # SIGINT can't be handled when polling, limit timeout to 1 second.
            msg = consumer.poll(1.0)
            if msg is None:
                print("over")
                continue

            case = json_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))

            if case is not None:
                #inserting the record in mongodb
                records.append(case.record)
                print("User record {}: car: {}\n"
                      .format(msg.key(), case))

                global count
                count = count + 1  # variable to print no of total records



        except KeyboardInterrupt:
            break

    consumer.close()
    if records:
        collection.insert_many(records)
    else:
        print("not dumped in db")

    print("total no of record pulled from this topic " +str(count))



main("time")
