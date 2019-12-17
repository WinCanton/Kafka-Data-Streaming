from dataclasses import dataclass, field
import json
import random
from datetime import datetime

from confluent_kafka import Consumer, Producer
from confluent_kafka.admin import AdminClient, NewTopic
from faker import Faker


faker = Faker()

BROKER_URL = "PLAINTEXT://localhost:9092"
TOPIC_NAME = "org.udacity.exercise4.purchases"


def produce(topic_name):
    """Produces data synchronously into the Kafka Topic
       See: https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    """

    p = Producer(
        {
            "bootstrap.servers": BROKER_URL,
            # TODO
            "client.id" : "myProducer",
            "linger.ms" : 1000,
            "compression.codec" : "lz4",
            "batch.num.messages" : 100,
            "queue.buffering.max.messages": 10000,
        }
    )

    start_time = datetime.utcnow()
    curr_iteration = 0

    while True:
        p.produce(topic_name, Purchase().serialize())
        if curr_iteration % 5000 == 0:
            elapsed = (datetime.utcnow() - start_time).seconds
            print(f"Messages sent: {curr_iteration} | Total elapsed seconds: {elapsed}")
        curr_iteration += 1
        p.poll(0)
        # Note: we call poll here to flush message delivery reports from Kafka.
        # At the moment, we are not interested about the details, so calling it with a timeout of 0s
        # means it returns immediately and has very little performance impact.


def main():
    """Checks for topic and creates the topic if it does not exist"""
    create_topic(TOPIC_NAME)
    try:
        produce(TOPIC_NAME)
    except KeyboardInterrupt as e:
        print("shutting down")


def create_topic(client):
    """Creates the topic with the given topic name"""
    client = AdminClient({"bootstrap.servers": BROKER_URL})
    futures = client.create_topics(
        [NewTopic(topic=TOPIC_NAME, num_partitions=5, replication_factor=1)]
    )
    for _, future in futures.items():
        try:
            future.result()
        except Exception as e:
            pass


@dataclass
class Purchase:
    username: str = field(default_factory=faker.user_name)
    currency: str = field(default_factory=faker.currency_code)
    amount: int = field(default_factory=lambda: random.randint(100, 200000))

    def serialize(self):
        """Serializes the object in JSON string format"""
        return json.dumps(
            {
                "username": self.username,
                "currency": self.currency,
                "amount": self.amount,
            }
        )


if __name__ == "__main__":
    main()
