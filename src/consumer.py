import json
import logging
import pika
from src.config import (
    get_connection_parameters,
    QUEUE_TASKS,
    QUEUE_DEAD_LETTER,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("RabbitMQConsumer")


class RabbitMQConsumer:
    def __init__(self, queue_name: str = QUEUE_TASKS):
        self.params = get_connection_parameters()
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

    def connect(self):
        self.connection = pika.BlockingConnection(self.params)
        self.channel = self.connection.channel()
        # Fair dispatch QoS: deliver only 1 message at a time to worker
        self.channel.basic_qos(prefetch_count=1)
        logger.info("Connected to queue '%s' with prefetch QoS = 1", self.queue_name)

    def on_message(self, channel, method, properties, body):
        try:
            payload = json.loads(body.decode("utf-8"))
            logger.info("Processing message from tag %s: %s", method.delivery_tag, payload)

            # Check if simulation triggers failure (for dead-letter testing)
            if payload.get("force_fail"):
                raise ValueError("Simulated catastrophic processing error")

            # Positive acknowledgment
            channel.basic_ack(delivery_tag=method.delivery_tag)
            logger.info("Successfully processed and ACKed message %s", method.delivery_tag)

        except Exception as e:
            logger.warning("Error processing message %s: %s. NACKing to DLX.", method.delivery_tag, e)
            # Negative acknowledgment without requeue -> routes immediately to Dead-Letter Exchange!
            channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

    def start_consuming(self):
        self.channel.basic_consume(
            queue=self.queue_name,
            on_message_callback=self.on_message,
            auto_ack=False,
        )
        logger.info("Awaiting messages in '%s'. Press CTRL+C to exit.", self.queue_name)
        try:
            self.channel.start_consuming()
        except KeyboardInterrupt:
            self.stop()

    def stop(self):
        if self.channel and self.channel.is_open:
            self.channel.stop_consuming()
        if self.connection and self.connection.is_open:
            self.connection.close()
        logger.info("Consumer stopped gracefully.")
