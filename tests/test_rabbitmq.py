import re
import pytest

def match_topic_routing_key(binding_pattern: str, routing_key: str) -> bool:
    """
    Simulates RabbitMQ topic exchange wildcard matching:
    * (star) matches exactly one word.
    # (hash) matches zero or more words.
    """
    regex_pattern = "^" + binding_pattern.replace(".", "\\.").replace("*", "[^.]+").replace("#", ".*") + "$"
    return bool(re.match(regex_pattern, routing_key))

def test_topic_exchange_wildcard_matching():
    # * matches exactly one word
    assert match_topic_routing_key("stage.*.completed", "stage.1.completed") is True
    assert match_topic_routing_key("stage.*.completed", "stage.core.completed") is True
    assert match_topic_routing_key("stage.*.completed", "stage.1.2.completed") is False

    # # matches zero or more words
    assert match_topic_routing_key("stage.#", "stage") is True
    assert match_topic_routing_key("stage.#", "stage.core") is True
    assert match_topic_routing_key("stage.#", "stage.core.internals.dlx") is True

    # Exact matching
    assert match_topic_routing_key("task.process", "task.process") is True
    assert match_topic_routing_key("task.process", "task.other") is False

def test_dead_letter_exchange_attributes():
    queue_args = {
        "x-dead-letter-exchange": "hub.dlx",
        "x-dead-letter-routing-key": "dead_letter",
        "x-message-ttl": 60000,
    }
    assert queue_args["x-dead-letter-exchange"] == "hub.dlx"
    assert queue_args["x-message-ttl"] == 60000

def test_exponential_backoff_calculation():
    def compute_backoff(attempt: int, base: float = 0.5, cap: float = 30.0) -> float:
        return min(cap, base * (2 ** attempt))

    assert compute_backoff(0) == 0.5
    assert compute_backoff(1) == 1.0
    assert compute_backoff(2) == 2.0
    assert compute_backoff(10) == 30.0  # Respects cap
