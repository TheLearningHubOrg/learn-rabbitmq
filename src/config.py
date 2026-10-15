import os
import pika

RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")

# Canonical Exchanges
EXCHANGE_DIRECT = "hub.direct"
EXCHANGE_FANOUT = "hub.fanout"
EXCHANGE_TOPIC = "hub.topic"
EXCHANGE_DLX = "hub.dlx"

# Canonical Queues
QUEUE_TASKS = "tasks.queue"
QUEUE_LOGS = "logs.queue"
QUEUE_ROADMAP = "roadmap.events.queue"
QUEUE_DEAD_LETTER = "dead_letter.queue"


def get_connection_parameters() -> pika.ConnectionParameters:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )
