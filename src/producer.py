import json
import logging
import pika
from src.config import (
    get_connection_parameters,
    EXCHANGE_DIRECT,
    EXCHANGE_FANOUT,
    EXCHANGE_TOPIC,
    EXCHANGE_DLX,
    QUEUE_TASKS,
    QUEUE_DEAD_LETTER,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RabbitMQProducer")


class RabbitMQProducer:
    def __init__(self):
        self.params = get_connection_parameters()
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        # Enable publisher confirms for delivery guarantees
        self.channel.confirm_delivery()
        self._setup_topology()
        logger.info("Connected to RabbitMQ with publisher confirms enabled.")

    def _setup_topology(self):
        # 1. Dead-Letter Exchange & Queue
        self.channel.exchange_declare(exchange=EXCHANGE_DLX, exchange_type="direct", durable=True)
        self.channel.queue_declare(queue=QUEUE_DEAD_LETTER, durable=True)
        self.channel.queue_bind(queue=QUEUE_DEAD_LETTER, exchange=EXCHANGE_DLX, routing_key="dead_letter")

        # 2. Direct Exchange & Task Queue with DLX configuration
        self.channel.exchange_declare(exchange=EXCHANGE_DIRECT, exchange_type="direct", durable=True)
        queue_args = {
            "x-dead-letter-exchange": EXCHANGE_DLX,
            "x-dead-letter-routing-key": "dead_letter",
            "x-message-ttl": 60000,  # 60s message TTL
        }
        self.channel.queue_declare(queue=QUEUE_TASKS, durable=True, arguments=queue_args)
        self.channel.queue_bind(queue=QUEUE_TASKS, exchange=EXCHANGE_DIRECT, routing_key="task.process")

        # 3. Fanout Exchange
        self.channel.exchange_declare(exchange=EXCHANGE_FANOUT, exchange_type="fanout", durable=True)

        # 4. Topic Exchange
        self.channel.exchange_declare(exchange=EXCHANGE_TOPIC, exchange_type="topic", durable=True)

    def publish_direct(self, message: dict, routing_key: str = "task.process") -> bool:
        body = json.dumps(message)
        properties = pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
            headers={"source": "TheLearningHub"},
        )
        try:
            self.channel.basic_publish(
                exchange=EXCHANGE_DIRECT,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=True,
            )
            logger.info("Published Direct Message [routing_key=%s]: %s", routing_key, message)
            return True
        except pika.exceptions.UnroutableError:
            logger.error("Message was rejected / unroutable!")
            return False

    def publish_fanout(self, broadcast_data: dict):
        body = json.dumps(broadcast_data)
        properties = pika.BasicProperties(content_type="application/json")
        self.channel.basic_publish(exchange=EXCHANGE_FANOUT, routing_key="", body=body, properties=properties)
        logger.info("Broadcasted Fanout Message: %s", broadcast_data)

    def publish_topic(self, event_data: dict, routing_key: str):
        body = json.dumps(event_data)
        properties = pika.BasicProperties(content_type="application/json")
        self.channel.basic_publish(exchange=EXCHANGE_TOPIC, routing_key=routing_key, body=body, properties=properties)
        logger.info("Published Topic Message [routing_key=%s]: %s", routing_key, event_data)

    def close(self):
        if self.connection and self.connection.is_open:
            self.connection.close()
            logger.info("RabbitMQ producer connection closed.")
