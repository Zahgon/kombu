"""Event loop implementation."""

from __future__ import annotations

import errno
import threading
from contextlib import contextmanager
from copy import copy
from queue import Empty
from time import sleep
from types import GeneratorType as generator

from vine import Thenable, promise

from kombu.log import get_logger
from kombu.utils.compat import fileno
from kombu.utils.eventio import ERR, READ, WRITE, poll
from kombu.utils.objects import cached_property

from .timer import Timer

__all__ = ('Hub', 'get_event_loop', 'set_event_loop')
logger = get_logger(__name__)

_current_loop: Hub | None = None

W_UNKNOWN_EVENT = """\
Received unknown event %r for fd %r, please contact support!\
"""


class Stop(BaseException):
    """Stops the event loop."""


def _raise_stop_error():
    raise Stop()


@contextmanager
def _dummy_context(*args, **kwargs):
    pass


def get_event_loop() -> Hub | None:
    """Get current event loop object."""
    return _current_loop


def set_event_loop(loop: Hub | None) -> Hub | None:
    """Set the current event loop object."""
    global _current_loop
    _current_loop = loop
    return loop


class Hub:
    """Event loop object.

    Arguments:
    ---------
        timer (kombu.asynchronous.Timer): Specify custom timer instance.
    """

    #: Flag set if reading from an fd will not block.
    READ = READ

    #: Flag set if writing to an fd will not block.
    WRITE = WRITE

    #: Flag set on error, and the fd should be read from asap.
    ERR = ERR

    #: List of callbacks to be called when the loop is exiting,
    #: applied with the hub instance as sole argument.
    on_close = None

    def __init__(self, timer=None):
        self.timer = timer if timer is not None else Timer()

        self.readers = {}
        self.writers = {}
        self.on_tick = set()
        self.on_close = set()
        self._ready = set()
        self._ready_lock = threading.Lock()

        self._running = False
        self._loop = None

        # The eventloop (in celery.worker.loops)
        # will merge fds in this set and then instead of calling
        # the callback for each ready fd it will call the
        # :attr:`consolidate_callback` with the list of ready_fds
        # as an argument.  This API is internal and is only
        # used by the multiprocessing pool to find inqueues
        # that are ready to write.
        self.consolidate = set()
        self.consolidate_callback = None

        self.propagate_errors = ()

        self._create_poller()

    @property
    def poller(self):
        pass

    @poller.setter
    def poller(self, value):
        pass

    def reset(self):
        pass

    def _create_poller(self):
        pass

    def _close_poller(self):
        if self._poller is not None:
            self._poller.close()
            self._poller = None
            self._register_fd = None
            self._unregister_fd = None

    def stop(self):
        pass

    def __repr__(self):
        return '<Hub@{:#x}: R:{} W:{}>'.format(
            id(self), len(self.readers), len(self.writers),
        )

    def fire_timers(self, min_delay=1, max_delay=10, max_timers=10,
                    propagate=()):
        pass

    def _remove_from_loop(self, fd):
        try:
            self._unregister(fd)
        finally:
            self._discard(fd)

    def add(self, fd, callback, flags, args=(), consolidate=False):
        fd = fileno(fd)
        try:
            self.poller.register(fd, flags)
        except ValueError:
            self._remove_from_loop(fd)
            raise
        else:
            dest = self.readers if flags & READ else self.writers
            if consolidate:
                self.consolidate.add(fd)
                dest[fd] = None
            else:
                dest[fd] = callback, args

    def remove(self, fd):
        fd = fileno(fd)
        self._remove_from_loop(fd)

    def run_forever(self):
        pass

    def run_once(self):
        pass

    def call_soon(self, callback, *args):
        if not isinstance(callback, Thenable):
            callback = promise(callback, args)
        with self._ready_lock:
            self._ready.add(callback)
        return callback

    def call_later(self, delay, callback, *args):
        return self.timer.call_after(delay, callback, args)

    def call_at(self, when, callback, *args):
        pass

    def call_repeatedly(self, delay, callback, *args):
        pass

    def add_reader(self, fds, callback, *args):
        return self.add(fds, callback, READ | ERR, args)

    def add_writer(self, fds, callback, *args):
        return self.add(fds, callback, WRITE, args)

    def remove_reader(self, fd):
        pass

    def remove_writer(self, fd):
        pass

    def _unregister(self, fd):
        try:
            self.poller.unregister(fd)
        except (AttributeError, KeyError, OSError):
            pass

    def _pop_ready(self):
        with self._ready_lock:
            ready = self._ready
            self._ready = set()
            return ready

    def close(self, *args):
        [self._unregister(fd) for fd in self.readers]
        self.readers.clear()
        [self._unregister(fd) for fd in self.writers]
        self.writers.clear()
        self.consolidate.clear()
        self._close_poller()
        for callback in self.on_close:
            callback(self)

        # Complete remaining todo before Hub close
        # Eg: Acknowledge message
        # To avoid infinite loop where one of the callables adds items
        # to self._ready (via call_soon or otherwise).
        # we create new list with current self._ready
        todos = self._pop_ready()
        for item in todos:
            item()

        # Clear global event loop variable if this hub is the current loop
        if _current_loop is self:
            set_event_loop(None)

    def _discard(self, fd):
        fd = fileno(fd)
        self.readers.pop(fd, None)
        self.writers.pop(fd, None)
        self.consolidate.discard(fd)

    def on_callback_error(self, callback, exc):
        pass

    def create_loop(self,
                    generator=generator, sleep=sleep, min=min, next=next,
                    Empty=Empty, StopIteration=StopIteration,
                    KeyError=KeyError, READ=READ, WRITE=WRITE, ERR=ERR):
        pass

    def repr_active(self):
        pass

    def repr_events(self, events):
        pass

    @cached_property
    def scheduler(self):
        pass

    @property
    def loop(self):
        pass
