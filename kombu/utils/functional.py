"""Functional Utilities."""

from __future__ import annotations

import inspect
import random
import threading
from collections import OrderedDict, UserDict
from collections.abc import Iterable, Mapping
from itertools import count, repeat
from time import sleep, time

from vine.utils import wraps

from .encoding import safe_repr as _safe_repr

__all__ = (
    'LRUCache', 'memoize', 'lazy', 'maybe_evaluate',
    'is_list', 'maybe_list', 'dictfilter', 'retry_over_time',
)

KEYWORD_MARK = object()


class ChannelPromise:

    def __init__(self, contract):
        self.__contract__ = contract

    def __call__(self):
        try:
            return self.__value__
        except AttributeError:
            pass
        value = self.__value__ = self.__contract__()
        return value

    def __repr__(self):
        try:
            return repr(self.__value__)
        except AttributeError:
            return f'<promise: 0x{id(self.__contract__):x}>'


class LRUCache(UserDict):
    """LRU Cache implementation using a doubly linked list to track access.

    Arguments:
    ---------
        limit (int): The maximum number of keys to keep in the cache.
            When a new key is inserted and the limit has been exceeded,
            the *Least Recently Used* key will be discarded from the
            cache.
    """

    def __init__(self, limit=None):
        self.limit = limit
        self.mutex = threading.RLock()
        self.data = OrderedDict()

    def __getitem__(self, key):
        with self.mutex:
            value = self[key] = self.data.pop(key)
            return value

    def update(self, *args, **kwargs):
        with self.mutex:
            data, limit = self.data, self.limit
            data.update(*args, **kwargs)
            if limit and len(data) > limit:
                # pop additional items in case limit exceeded
                for _ in range(len(data) - limit):
                    data.popitem(last=False)

    def popitem(self, last=True):
        with self.mutex:
            return self.data.popitem(last)

    def __setitem__(self, key, value):
        # remove least recently used key.
        with self.mutex:
            if self.limit and len(self.data) >= self.limit:
                self.data.pop(next(iter(self.data)))
            self.data[key] = value

    def __iter__(self):
        return iter(self.data)

    def _iterate_items(self):
        pass
    iteritems = _iterate_items

    def _iterate_values(self):
        pass

    itervalues = _iterate_values

    def _iterate_keys(self):
        # userdict.keys in py3k calls __getitem__
        pass
    iterkeys = _iterate_keys

    def incr(self, key, delta=1):
        pass

    def __getstate__(self):
        d = dict(vars(self))
        d.pop('mutex')
        return d

    def __setstate__(self, state):
        self.__dict__ = state
        self.mutex = threading.RLock()

    keys = _iterate_keys
    values = _iterate_values
    items = _iterate_items


def memoize(maxsize=None, keyfun=None, Cache=LRUCache):
    """Decorator to cache function return value."""
    def _memoize(fun):
        pass

    return _memoize


class lazy:
    """Holds lazy evaluation.

    Evaluated when called or if the :meth:`evaluate` method is called.
    The function is re-evaluated on every call.

    Overloaded operations that will evaluate the promise:
        :meth:`__str__`, :meth:`__repr__`, :meth:`__cmp__`.
    """

    def __init__(self, fun, *args, **kwargs):
        self._fun = fun
        self._args = args
        self._kwargs = kwargs

    def __call__(self):
        return self.evaluate()

    def evaluate(self):
        return self._fun(*self._args, **self._kwargs)

    def __str__(self):
        return str(self())

    def __repr__(self):
        return repr(self())

    def __eq__(self, rhs):
        return self() == rhs

    def __ne__(self, rhs):
        return self() != rhs

    def __deepcopy__(self, memo):
        memo[id(self)] = self
        return self

    def __reduce__(self):
        return (self.__class__, (self._fun,), {'_args': self._args,
                                               '_kwargs': self._kwargs})


def maybe_evaluate(value):
    """Evaluate value only if value is a :class:`lazy` instance."""
    if isinstance(value, lazy):
        return value.evaluate()
    return value


def is_list(obj, scalars=(Mapping, str), iters=(Iterable,)):
    """Return true if the object is iterable.

    Note:
    ----
        Returns false if object is a mapping or string.
    """
    pass


def maybe_list(obj, scalars=(Mapping, str)):
    """Return list of one element if ``l`` is a scalar."""
    pass


def dictfilter(d=None, **kw):
    """Remove all keys from dict ``d`` whose value is :const:`None`."""
    d = kw if d is None else (dict(d, **kw) if kw else d)
    return {k: v for k, v in d.items() if v is not None}


def shufflecycle(it):
    pass


def fxrange(start=1.0, stop=None, step=1.0, repeatlast=False):
    cur = start * 1.0
    while 1:
        if not stop or cur <= stop:
            yield cur
            cur += step
        else:
            if not repeatlast:
                break
            yield cur - step


def fxrangemax(start=1.0, stop=None, step=1.0, max=100.0):
    pass


def retry_over_time(fun, catch, args=None, kwargs=None, errback=None,
                    max_retries=None, interval_start=2, interval_step=2,
                    interval_max=30, callback=None, timeout=None):
    """Retry the function over and over until max retries is exceeded.

    For each retry we sleep a for a while before we try again, this interval
    is increased for every retry until the max seconds is reached.

    Arguments:
    ---------
        fun (Callable): The function to try
        catch (Tuple[BaseException]): Exceptions to catch, can be either
            tuple or a single exception class.

    Keyword Arguments:
    -----------------
        args (Tuple): Positional arguments passed on to the function.
        kwargs (Dict): Keyword arguments passed on to the function.
        errback (Callable): Callback for when an exception in ``catch``
            is raised.  The callback must take three arguments:
            ``exc``, ``interval_range`` and ``retries``, where ``exc``
            is the exception instance, ``interval_range`` is an iterator
            which return the time in seconds to sleep next, and ``retries``
            is the number of previous retries.
        max_retries (int): Maximum number of retries before we give up.
            If neither of this and timeout is set, we will retry forever.
            If one of this and timeout is reached, stop.
        interval_start (float): How long (in seconds) we start sleeping
            between retries.
        interval_step (float): By how much the interval is increased for
            each retry.
        interval_max (float): Maximum number of seconds to sleep
            between retries.
        timeout (int): Maximum seconds waiting before we give up.
    """
    kwargs = {} if not kwargs else kwargs
    args = [] if not args else args
    interval_range = fxrange(interval_start,
                             interval_max + interval_start,
                             interval_step, repeatlast=True)
    end = time() + timeout if timeout else None
    for retries in count():
        try:
            return fun(*args, **kwargs)
        except catch as exc:
            if max_retries is not None and retries >= max_retries:
                raise
            if end and time() > end:
                raise
            if callback:
                callback()
            tts = float(errback(exc, interval_range, retries) if errback
                        else next(interval_range))
            if tts:
                for _ in range(int(tts)):
                    if callback:
                        callback()
                    sleep(1.0)
                # sleep remainder after int truncation above.
                sleep(abs(int(tts) - tts))


def reprkwargs(kwargs, sep=', ', fmt='{0}={1}'):
    pass


def reprcall(name, args=(), kwargs=None, sep=', '):
    pass


def accepts_argument(func, argument_name):
    pass


# Compat names (before kombu 3.0)
promise = lazy
maybe_promise = maybe_evaluate
