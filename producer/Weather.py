import argparse
from uuid import uuid4
from six.moves import input
import numpy as np
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
# from confluent_kafka.schema_registry import *
import pandas as pd

count = 0
from typing import List

FILE_PATH = "../csv/Weather.csv"
API_KEY = '3DE33YMBIGNYT7DL'
ENDPOINT_SCHEMA_URL = 'https://psrc-ko92v.us-east-2.aws.confluent.cloud'
API_SECRET_KEY = 'Al/PSF0fgjNf3TOan1rnOpJThbg52/fk24dQkSo3uUzJIkHEoZ/XM1xIHCGrFQuh'
BOOTSTRAP_SERVER = 'pkc-ymrq7.us-east-2.aws.confluent.cloud:9092'

SECURITY_PROTOCOL = 'SASL_SSL'
SSL_MACHENISM = 'PLAIN'
SCHEMA_REGISTRY_API_KEY = 'QZ4IELQGPAOCHVUH'
SCHEMA_REGISTRY_API_SECRET = 'hf/WuZYzMIPOFhwRAXn3T8DWaR//wopasz4m8uBa+jV3fjFNrH7CQOFFYGAZQGxJ'

class Weather:
    def __init__(self, record: dict):
        for k, v in record.items():
            self.__setattr__(k, v)

        self.record = record

    @staticmethod
    def dict_to_Weather(data: dict, ctx):
        return Weather(record=data)

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


def Weather_to_dict(Weather, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    # print(car.record)
    return Weather.record


# method to get an instane of the class
def getinstance(filePath):
    df = pd.read_csv(filePath)

    df=df.astype(str,errors="ignore")

    df = df.to_dict("records")

    c1 = []
    for values in df:
           #print(values)

           ca = Weather(values)
           c1.append(ca)

    return c1


def Weather_to_dict(Weather, ctx):
    """
    Returns a dict representation of a User instance for serialization.
    Args:
        user (User): User instance.
        ctx (SerializationContext): Metadata pertaining to the serialization
            operation.
    Returns:
        dict: Dict populated with user attributes to be serialized.
    """

    # User._address must not be serialized; omit from dict
    # print(car.record)
    return Weather.record


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

    schema_registry_conf = schema_config()
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)
    my_schema = schema_registry_client.get_latest_version("weather-value")
    string_serializer = StringSerializer('utf_8')

    my_str = my_schema.schema.schema_str
    print(my_str)
    json_serializer = JSONSerializer(my_str, schema_registry_client, Weather_to_dict)

    producer = Producer(sasl_conf())

    print("Producing user records to topic {}. ^C to exit.".format(topic))
    # while True:
    # Serve on_delivery callbacks from previous calls to produce()
    producer.poll(0.0)
    try:
        for data in getinstance(FILE_PATH):
            # print(car)
            producer.produce(topic,
                             key=string_serializer(str(uuid4()),Weather_to_dict),
                             value=json_serializer(data, SerializationContext(topic, MessageField.VALUE)),
                             on_delivery=delivery_report)

    except KeyboardInterrupt:
        pass
    except ValueError:
        print("Invalid input, discarding record...")
        pass

    print("\nFlushing records...")
    producer.flush()
    print("total no of recorder pushed to the topic :" + str(count))



main("weather")
