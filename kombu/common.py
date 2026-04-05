"""Common Utilities."""

from __future__ import annotations

import os
import socket
import threading
from collections import deque
from contextlib import contextmanager
from functools import partial
from itertools import count
from uuid import NAMESPACE_OID, uuid3, uuid4, uuid5

from amqp import ChannelError, RecoverableConnectionError

from .entity import Exchange, Queue
from .log import get_logger
from .serialization import registry as serializers
from .utils.compat import get_gevent_concurrent_error
from .utils.uuid import uuid

__all__ = ('Broadcast', 'maybe_declare', 'uuid',
           'itermessages', 'send_reply',
           'collect_replies', 'insured', 'drain_consumer',
           'eventloop')

#: Prefetch count can't exceed short.
PREFETCH_COUNT_MAX = 0xFFFF

logger = get_logger(__name__)

_node_id = None


def get_node_id():
    pass


def generate_oid(node_id, process_id, thread_id, instance):
    pass


def oid_from(instance, threads=True):
    pass


class Broadcast(Queue):
    """Broadcast queue.

    Convenience class used to define broadcast queues.

    Every queue instance will have a unique name,
    and both the queue and exchange is configured with auto deletion.

    Arguments:
    ---------
        name (str): This is used as the name of the exchange.
        queue (str): By default a unique id is used for the queue
            name for every consumer.  You can specify a custom
            queue name here.
        unique (bool): Always create a unique queue
            even if a queue name is supplied.
        **kwargs (Any): See :class:`~kombu.Queue` for a list
            of additional keyword arguments supported.
    """

    attrs = Queue.attrs + (('queue', None),)

    def __init__(self,
                 name=None,
                 queue=None,
                 unique=False,
                 auto_delete=True,
                 exchange=None,
                 alias=None,
                 **kwargs):
        if unique:
            queue = '{}.{}'.format(queue or 'bcast', uuid())
        else:
            queue = queue or f'bcast.{uuid()}'
        super().__init__(
            alias=alias or name,
            queue=queue,
            name=queue,
            auto_delete=auto_delete,
            exchange=(exchange if exchange is not None
                      else Exchange(name, type='fanout')),
            **kwargs
        )


def declaration_cached(entity, channel):
    pass


def maybe_declare(entity, channel=None, retry=False, **retry_policy):
    """Declare entity (cached)."""
    if retry:
        return _imaybe_declare(entity, channel, **retry_policy)
    return _maybe_declare(entity, channel)


def _ensure_channel_is_bound(entity, channel):
    """Make sure the channel is bound to the entity.

    :param entity: generic kombu nomenclature, generally an exchange or queue
    :param channel: channel to bind to the entity
    :return: the updated entity
    """
    is_bound = entity.is_bound
    if not is_bound:
        if not channel:
            raise ChannelError(
                f"Cannot bind channel {channel} to entity {entity}")
        entity = entity.bind(channel)
    return entity


def _maybe_declare(entity, channel):
    # _maybe_declare sets name on original for autogen queues
    orig = entity

    _ensure_channel_is_bound(entity, channel)

    if channel is None or channel.connection is None:
        # If this was called from the `ensure()` method then the channel could have been invalidated
        # and the correct channel was re-bound to the entity by calling the `entity.revive()` method.
        if not entity.is_bound:
            raise ChannelError(
                f"channel is None and entity {entity} not bound.")
        channel = entity.channel

    declared = ident = None
    if channel.connection and entity.can_cache_declaration:
        declared = channel.connection.client.declared_entities
        ident = hash(entity)
        if ident in declared:
            return False

    if not channel.connection:
        raise RecoverableConnectionError('channel disconnected')
    entity.declare(channel=channel)
    if declared is not None and ident:
        declared.add(ident)
    if orig is not None:
        orig.name = entity.name
    return True


def _imaybe_declare(entity, channel, **retry_policy):
    entity = _ensure_channel_is_bound(entity, channel)

    if not entity.channel.connection:
        raise RecoverableConnectionError('channel disconnected')

    return entity.channel.connection.client.ensure(
        entity, _maybe_declare, **retry_policy)(entity, channel)


def drain_consumer(consumer, limit=1, timeout=None, callbacks=None):
    """Drain messages from consumer instance."""
    pass


def itermessages(conn, channel, queue, limit=1, timeout=None,
                 callbacks=None, **kwargs):
    """Iterator over messages."""
    pass


def eventloop(conn, limit=None, timeout=None, ignore_timeouts=False):
    """Best practice generator wrapper around ``Connection.drain_events``.

    Able to drain events forever, with a limit, and optionally ignoring
    timeout errors (a timeout of 1 is often used in environments where
    the socket can get "stuck", and is a best practice for Kombu consumers).

    ``eventloop`` is a generator.

    Examples
    --------
        >>> from kombu.common import eventloop

        >>> def run(conn):
        ...     it = eventloop(conn, timeout=1, ignore_timeouts=True)
        ...     next(it)   # one event consumed, or timed out.
        ...
        ...     for _ in eventloop(conn, timeout=1, ignore_timeouts=True):
        ...         pass  # loop forever.

    It also takes an optional limit parameter, and timeout errors
    are propagated by default::

        for _ in eventloop(connection, limit=1, timeout=1):
            pass

    See Also
    --------
        :func:`itermessages`, which is an event loop bound to one or more
        consumers, that yields any messages received.
    """
    pass


def send_reply(exchange, req, msg,
               producer=None, retry=False, retry_policy=None, **props):
    """Send reply for request.

    Arguments:
    ---------
        exchange (kombu.Exchange, str): Reply exchange
        req (~kombu.Message): Original request, a message with
            a ``reply_to`` property.
        producer (kombu.Producer): Producer instance
        retry (bool): If true must retry according to
            the ``reply_policy`` argument.
        retry_policy (Dict): Retry settings.
        **props (Any): Extra properties.
    """
    pass


def collect_replies(conn, channel, queue, *args, **kwargs):
    """Generator collecting replies from ``queue``."""
    pass


def _ensure_errback(exc, interval):
    pass


@contextmanager
def _ignore_errors(conn):
    pass


def ignore_errors(conn, fun=None, *args, **kwargs):
    """Ignore connection and channel errors.

    The first argument must be a connection object, or any other object
    with ``connection_error`` and ``channel_error`` attributes.

    Can be used as a function:

    .. code-block:: python

        def example(connection):
            ignore_errors(connection, consumer.channel.close)

    or as a context manager:

    .. code-block:: python

        def example(connection):
            with ignore_errors(connection):
                consumer.channel.close()


    Note:
    ----
        Connection and channel errors should be properly handled,
        and not ignored.  Using this function is only acceptable in a cleanup
        phase, like when a connection is lost or at shutdown.
    """
    pass


def revive_connection(connection, channel, on_revive=None):
    pass


def insured(pool, fun, args, kwargs, errback=None, on_revive=None, **opts):
    """Function wrapper to handle connection errors.

    Ensures function performing broker commands completes
    despite intermittent connection failures.
    """
    pass


class QoS:
    """Thread safe increment/decrement of a channels prefetch_count.

    Arguments:
    ---------
        callback (Callable): Function used to set new prefetch count,
            e.g. ``consumer.qos`` or ``channel.basic_qos``.  Will be called
            with a single ``prefetch_count`` keyword argument.
        initial_value (int): Initial prefetch count value..
        max_prefetch (int or None): Maximum allowed prefetch count. If specified
            as an integer, increment_eventually will not allow the value to exceed this limit.
            If None (the default), there is no upper limit on the prefetch count.

    Example:
    -------
        >>> from kombu import Consumer, Connection
        >>> connection = Connection('amqp://')
        >>> consumer = Consumer(connection)
        >>> qos = QoS(consumer.qos, initial_prefetch_count=2)
        >>> qos.update()  # set initial

        >>> qos.value
        2

        >>> def in_some_thread():
        ...     qos.increment_eventually()

        >>> def in_some_other_thread():
        ...     qos.decrement_eventually()

        >>> while 1:
        ...    if qos.prev != qos.value:
        ...        qos.update()  # prefetch changed so update.

    It can be used with any function supporting a ``prefetch_count`` keyword
    argument::

        >>> channel = connection.channel()
        >>> QoS(channel.basic_qos, 10)


        >>> def set_qos(prefetch_count):
        ...     print('prefetch count now: %r' % (prefetch_count,))
        >>> QoS(set_qos, 10)
    """

    prev = None

    def __init__(self, callback, initial_value, max_prefetch=None):
        self.callback = callback
        self._mutex = threading.RLock()
        self.value = initial_value or 0
        self.max_prefetch = max_prefetch

    def increment_eventually(self, n=1):
        """Increment the value, but do not update the channels QoS.

        Note:
        ----
            The MainThread will be responsible for calling :meth:`update`
            when necessary. If max_prefetch is set, the value will not
            exceed this limit.
        """
        with self._mutex:
            if self.value:
                new_value = self.value + max(n, 0)
                if self.max_prefetch is not None and new_value > self.max_prefetch:
                    new_value = self.max_prefetch
                self.value = new_value
        return self.value

    def decrement_eventually(self, n=1):
        """Decrement the value, but do not update the channels QoS.

        Note:
        ----
            The MainThread will be responsible for calling :meth:`update`
            when necessary.
        """
        pass

    def set(self, pcount):
        """Set channel prefetch_count setting."""
        pass

    def update(self):
        """Update prefetch count with current value."""
        with self._mutex:
            return self.set(self.value)
