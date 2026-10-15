import time
from src.producer import RabbitMQProducer

def main():
    print("=" * 60)
    print("  Learn RabbitMQ: AMQP 0-9-1 Messaging Architecture Demo")
    print("=" * 60)

    try:
        producer = RabbitMQProducer()
        producer.connect()

        # 1. Direct Exchange
        print("\n1. Publishing task to Direct Exchange (hub.direct)...")
        producer.publish_direct(
            {"task_id": 101, "action": "TRANSCODE_VIDEO", "priority": "HIGH"},
            routing_key="task.process"
        )

        # 2. Fanout Exchange
        print("\n2. Broadcasting event to Fanout Exchange (hub.fanout)...")
        producer.publish_fanout(
            {"event": "CLUSTER_STATE_CHANGE", "status": "ALL_NODES_HEALTHY", "timestamp": time.time()}
        )

        # 3. Topic Exchange with Wildcards
        print("\n3. Publishing events to Topic Exchange (hub.topic)...")
        producer.publish_topic(
            {"stage": 1, "topic": "Architecture & Topology", "status": "COMPLETED"},
            routing_key="stage.1.completed"
        )
        producer.publish_topic(
            {"stage": 4, "topic": "Dead Letter Exchanges", "status": "IN_PROGRESS"},
            routing_key="stage.4.progress"
        )

        # 4. Triggering Dead-Letter Exchange via simulated error
        print("\n4. Publishing message configured to fail (routes to DLX on NACK)...")
        producer.publish_direct(
            {"task_id": 999, "action": "INVALID_JOB", "force_fail": True},
            routing_key="task.process"
        )

        producer.close()
        print("\n[SUCCESS] RabbitMQ Demonstration Completed Successfully!")

    except Exception as e:
        print(f"\n[RabbitMQ Notice] Could not connect to live broker at localhost:5672 ({e}).")
        print("To launch a live RabbitMQ broker, run: docker compose up -d")

if __name__ == "__main__":
    main()
