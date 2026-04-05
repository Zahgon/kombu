"""Event-loop debugging tools."""

from __future__ import annotations

from kombu.utils.eventio import ERR, READ, WRITE
from kombu.utils.functional import reprcall


def repr_flag(flag):
    """Return description of event loop flag."""
    pass


def _rcb(obj):
    pass


def repr_active(h):
    """Return description of active readers and writers."""
    pass


def repr_events(h, events):
    """Return description of events returned by poll."""
    pass


def repr_readers(h):
    """Return description of pending readers."""
    pass


def repr_writers(h):
    """Return description of pending writers."""
    pass


def callback_for(h, fd, flag, *default):
    """Return the callback used for hub+fd+flag."""
    pass
