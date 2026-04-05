"""Text Utilities."""
# flake8: noqa


from __future__ import annotations

from difflib import SequenceMatcher
from typing import Iterable, Iterator

from kombu import version_info_t


def escape_regex(p, white=''):
    # type: (str, str) -> str
    """Escape string for use within a regular expression."""
    # what's up with re.escape? that code must be neglected or something
    return ''.join(c if c.isalnum() or c in white
                   else ('\\000' if c == '\000' else '\\' + c)
                   for c in p)


def fmatch_iter(needle: str, haystack: Iterable[str], min_ratio: float = 0.6) -> Iterator[tuple[float, str]]:
    """Fuzzy match: iteratively.

    Yields
    ------
        Tuple: of ratio and key.
    """
    pass


def fmatch_best(needle: str, haystack: Iterable[str], min_ratio: float = 0.6) -> str | None:
    """Fuzzy match - Find best match (scalar)."""
    pass


def version_string_as_tuple(s: str) -> version_info_t:
    """Convert version string to version info tuple."""
    pass


def _unpack_version(
    major: str,
    minor: str | int = 0,
    micro: str | int = 0,
    releaselevel: str = '',
    serial: str = ''
) -> version_info_t:
    pass


def _splitmicro(micro: str, releaselevel: str = '', serial: str = '') -> tuple[int, str, str]:
    pass
