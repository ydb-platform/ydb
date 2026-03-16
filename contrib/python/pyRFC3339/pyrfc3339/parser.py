import re
import sys
from datetime import datetime, timezone

from .utils import datetime_utcoffset


def parse(timestamp: str, utc: bool = False, produce_naive: bool = False) -> datetime:
    """
    Parse an :RFC:`3339`-formatted timestamp and return a :class:`datetime.datetime`.

    If the timestamp is presented in UTC, then the :attr:`~datetime.datetime.tzinfo` attribute of the
    returned `datetime` will be set to :attr:`datetime.timezone.utc`.

    >>> parse('2009-01-01T10:01:02Z')
    datetime.datetime(2009, 1, 1, 10, 1, 2, tzinfo=datetime.timezone.utc)

    Otherwise, a :class:`datetime.timezone` instance is created with the appropriate offset, and
    the :attr:`~datetime.datetime.tzinfo` attribute of the returned :class:`~datetime.datetime` is set to that value.

    >>> parse('2009-01-01T14:01:02-04:00')
    datetime.datetime(2009, 1, 1, 14, 1, 2, tzinfo=datetime.timezone(datetime.timedelta(days=-1, seconds=72000)))

    However, if :meth:`parse()`  is called with :python:`utc=True`, then the returned
    :class:`~datetime.datetime` will be normalized to UTC (and its :attr:`~datetime.datetime.tzinfo` attribute set to
    :attr:`~datetime.timezone.utc`), regardless of the input timezone.

    >>> parse('2009-01-01T06:01:02-04:00', utc=True)
    datetime.datetime(2009, 1, 1, 10, 1, 2, tzinfo=datetime.timezone.utc)

    As parsing is delegated to :meth:`datetime.datetime.fromisoformat()`, certain
    timestamps which do not strictly adhere to :RFC:`3339` are nonetheless accepted.

    >>> parse('2009-01-01T06:01:02')
    datetime.datetime(2009, 1, 1, 6, 1, 2)

    Exceptions will, however, be thrown for blatantly invalid input:

    >>> parse('2009-01-01T25:01:02Z')
    Traceback (most recent call last):
    ...
    ValueError: hour must be in 0..23

    :param str timestamp: the :RFC:`3339` timestamp to be parsed
    :param bool utc: :const:`True` to normalize the timestamp to UTC; :const:`False` otherwise. Defaults to :const:`False`.
    :param bool produce_naive: :const:`True` if the produced :class:`~datetime.datetime` instance should
                               not have a timezone attached (that is, be 'naive'); :const:`False` otherwise.
                               Defaults to :const:`False`.
    :return: the parsed timestamp
    :rtype: datetime.datetime

    """

    # Python does not recognize "Z" as an alias for "+00:00", so we perform the
    # substitution here.
    timestamp = re.sub("Z$", "+00:00", timestamp, flags=re.IGNORECASE)

    # Python releases prior to 3.11 only support three or six digits of fractional
    # seconds. RFC 3339 is more lenient, so pad to six digits and truncate any
    # excessive digits.
    # This can be removed in October 2026, once Python 3.10 and earlier
    # have been retired.
    # noinspection PyUnreachableCode
    if sys.version_info < (3, 11):
        timestamp = re.sub(
            r"(\.)([0-9]+)(?=[+\-][0-9]{2}:[0-9]{2}$)",
            lambda match: match.group(1) + match.group(2).ljust(6, "0")[:6],
            timestamp,
        )

    dt_out = datetime.fromisoformat(timestamp)

    if utc:
        dt_out = dt_out.astimezone(timezone.utc)

    if produce_naive:
        if datetime_utcoffset(dt_out) == 0:
            dt_out = dt_out.replace(tzinfo=None)
        else:
            raise ValueError("cannot produce a naive datetime from a local timestamp")

    return dt_out
