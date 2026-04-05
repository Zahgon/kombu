"""Native Delayed Delivery API.

Only relevant for RabbitMQ.
"""
from __future__ import annotations

from kombu import Connection, Exchange, Queue, binding
from kombu.log import get_logger

logger = get_logger(__name__)

MAX_NUMBER_OF_BITS_TO_USE = 28
MAX_LEVEL = MAX_NUMBER_OF_BITS_TO_USE - 1
CELERY_DELAYED_DELIVERY_EXCHANGE = "celery_delayed_delivery"


def level_name(level: int) -> str:
    """Generates the delayed queue/exchange name based on the level."""
    pass


def declare_native_delayed_delivery_exchanges_and_queues(connection: Connection, queue_type: str) -> None:
    """Declares all native delayed delivery exchanges and queues."""
    pass


def bind_queue_to_native_delayed_delivery_exchange(connection: Connection, queue: Queue) -> None:
    """Bind a queue to the native delayed delivery exchange.

    When a message arrives at the delivery exchange, it must be forwarded to
    the original exchange and queue. To accomplish this, the function retrieves
    the exchange or binding objects associated with the queue and binds them to
    the delivery exchange.


    :param connection: The connection object used to create and manage the channel.
    :type connection: Connection
    :param queue: The queue to be bound to the native delayed delivery exchange.
    :type queue: Queue

    Warning:
    -------
        If a direct exchange is detected, a warning will be logged because
        native delayed delivery does not support direct exchanges.
    """
    pass


def calculate_routing_key(countdown: int, routing_key: str) -> str:
    """Calculate the routing key for publishing a delayed message based on the countdown."""
    pass
