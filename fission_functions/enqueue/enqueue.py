import json
from typing import Dict, Any, Optional
from flask import current_app, request
import redis


def config(k: str) -> str:
    """Reads configuration from file."""
    with open(f"/configs/default/shared-data/{k}", "r") as f:
        return f.read()


def main() -> str:
    """Message queue producer for Redis streaming.

    Handles:
    - Redis connection pooling
    - JSON payload serialization
    - Topic-based message routing via headers
    - Message size logging

    Returns:
        'OK' with HTTP 200 on successful enqueue

    Raises:
        redis.RedisError: For connection/operation failures
        JSONDecodeError: If invalid payload received
    """
    req: Request = request

    # Extract routing parameters
    topic: Optional[str] = req.headers.get("X-Fission-Params-Topic")
    json_data: Dict[str, Any] = req.get_json()

    # Initialize Redis client with type annotation
    redis_client: redis.StrictRedis = redis.StrictRedis(
        host=config("REDIS_HOST"), socket_connect_timeout=5, decode_responses=False
    )

    if not json_data:
        current_app.logger.info(f"Skipped enqueue: empty list for topic {topic}")
        return "Skipped empty payload", 204

    # Publish message to queue
    redis_client.lpush(topic, json.dumps(json_data).encode("utf-8"))

    # Structured logging with message metrics
    current_app.logger.info(
        f"Enqueued to {topic} topic - Payload size: {len(json_data)} bytes"
    )

    return "OK"
