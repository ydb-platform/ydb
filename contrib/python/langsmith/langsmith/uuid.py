"""Public UUID v7 helpers.

These helpers expose utilities for generating UUID v7 identifiers in user code.
"""

from __future__ import annotations

import datetime as _dt
import uuid as _uuid

from ._internal._uuid import uuid7 as _uuid7


def uuid7() -> _uuid.UUID:
    """Generate a random UUID v7.

    Returns:
        uuid.UUID: A random, RFC 9562-compliant UUID v7.
    """
    return _uuid7()


def uuid7_from_datetime(dt: _dt.datetime) -> _uuid.UUID:
    """Generate a UUID v7 from a datetime.

    Args:
        dt: A timezone-aware datetime. If naive, it is treated as UTC.

    Returns:
        uuid.UUID: A UUID v7 whose timestamp corresponds to the provided time.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=_dt.timezone.utc)
    nanoseconds = int(dt.timestamp() * 1_000_000_000)
    return _uuid7(nanoseconds)


__all__ = ["uuid7", "uuid7_from_datetime"]
