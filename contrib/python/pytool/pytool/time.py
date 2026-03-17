"""
This module contains time related things that make life easier.
"""

# The regular 'import time' fails, because for some insane reason, Python lets
# a module import itself. This is a work around to import the non-relative
# module.
import importlib
import calendar
import datetime

from pytool.lang import singleton

# Importing the system module
time = importlib.import_module("time")


class Timer(object):
    """
    This is a simple timer class.

    ::

        timer = pytool.time.Timer()
        for i in (1, 2, 3):
            sleep(i)
            print timer.mark(), "elapsed since last mark or start"
        print timer.elapsed, "total elapsed"

    """

    def __init__(self):
        self._start = utcnow()
        self._last = self._start

    def mark(self):
        """
        Return a :class:`~datetime.datetime.timedelta` of the time elapsed
        since the last mark or start.

        """
        mark, self._last = self._last, utcnow()
        return self._last - mark

    @property
    def elapsed(self):
        """
        Return a :class:`~datetime.datetime.timedelta` of the time elapsed
        since the start.

        """
        return utcnow() - self._start


@singleton
class UTC(datetime.tzinfo):
    """UTC timezone. This is necessary since Python doesn't include any
    explicit timezone objects in the standard library. This can be used
    to create timezone-aware datetime objects, which are a pain to work
    with, but a necessary evil sometimes.

    ::

        from datetime import datetime
        from pytool.time import UTC

        utc_now = datetime.now(UTC())

    """

    @property
    def _utcoffset(self) -> datetime.timedelta:
        """Helps make this work with pytz."""
        return datetime.timedelta(0)

    def utcoffset(self, stamp) -> datetime.timedelta:
        return datetime.timedelta(0)

    def tzname(self, stamp) -> str:
        return "UTC"

    def dst(self, stamp) -> datetime.timedelta:
        return datetime.timedelta(0)

    def __repr__(self) -> str:
        return "UTC()"

    def __reduce__(self) -> tuple:
        # This method makes the UTC object pickleable
        return UTC, ()


def utc(
    year, month, day, hour=0, minute=0, second=0, microsecond=0
) -> datetime.datetime:
    """Return a timezone-aware datetime object for the given UTC time.

    :param int year: Year
    :param int month: Month
    :param int day: Day
    :param int hour: Hour (default: ``0``)
    :param int minute: Minute (default: ``0``)
    :param int second: Second (default: ``0``)
    :param int microsecond: Microsecond (default: ``0``)
    :returns: UTC datetime object

    """
    return datetime.datetime(
        year, month, day, hour, minute, second, microsecond, tzinfo=UTC()
    )


def is_dst(stamp):
    """Return ``True`` if `stamp` is daylight savings.

    :param datetime stamp: Datetime
    :returns: ``True`` if `stamp` is daylight savings, otherwise ``False``.

    """
    return time.localtime(time.mktime(stamp.timetuple())).tm_isdst == 1


def utcnow() -> datetime.datetime:
    """Return the current UTC time as a timezone-aware datetime.

    :returns: The current UTC time

    """
    return datetime.datetime.now(UTC())


def fromutctimestamp(stamp) -> datetime.datetime:
    """Return a timezone-aware datetime object from a UTC unix timestamp.

    :param float stamp: Unix timestamp in UTC
    :returns: UTC datetime object

    ::

        import time
        from pytool.time import fromutctimestamp

        utc_datetime = fromutctimestamp(time.time())

    """
    decimal = int(round(10**6 * (stamp - int(stamp))))
    stamp = time.gmtime(stamp)
    new_stamp = datetime.datetime(*(stamp[:6] + (decimal, UTC())))
    # Correct for Python's fucking terrible handling of daylight savings
    if stamp.tm_isdst:
        new_stamp -= datetime.timedelta(hours=1)
    return new_stamp


def toutctimestamp(stamp) -> datetime.datetime:
    """Converts a naive datetime object to a UTC unix timestamp. This has an
    advantage over `time.mktime` in that it preserves the decimal portion
    of the timestamp when converting.

    :param datetime stamp: Datetime to convert
    :returns: Unix timestamp as a ``float``

    ::

        from datetime import datetime
        from pytool.time import toutctimestamp

        utc_stamp = toutctimestamp(datetime.now())

    """
    decimal = 1.0 * stamp.microsecond / 10**6
    if stamp.tzinfo:
        # Hooray, it's timezone aware, we are saved!
        return calendar.timegm(stamp.utctimetuple()) + decimal

    # We don't have a timezone... shit
    return time.mktime(stamp.timetuple()) + decimal


def as_utc(stamp) -> datetime.datetime:
    """Converts any datetime (naive or aware) to UTC time.

    :param datetime stamp: Datetime to convert
    :returns: `stamp` as UTC time

    ::

        from datetime import datetime
        from pytool.time import as_utc

        utc_datetime = as_utc(datetime.now())

    """
    return fromutctimestamp(toutctimestamp(stamp))


def trim_time(stamp) -> datetime.datetime:
    """Trims the time portion off of `stamp`, leaving the date intact.
    Returns a datetime of the same date, set to 00:00:00 hours. Preserves
    timezone information.

    :param datetime stamp: Timestamp to trim
    :returns: Trimmed timestamp

    """
    return datetime.datetime(*stamp.date().timetuple()[:-3], tzinfo=stamp.tzinfo)


def week_start(stamp) -> datetime.datetime:
    """Return the start of the week containing `stamp`.

    .. versionchanged:: 2.0
       Preserves timezone information.

    :param datetime stamp: Timestamp
    :returns: A datetime for 00:00 Monday of the given week

    """
    stamp = stamp - datetime.timedelta(days=stamp.weekday())
    stamp = trim_time(stamp)
    return stamp


def week_seconds(stamp) -> int:
    """Return `stamp` converted to seconds since 00:00 Monday.

    :param datetime stamp: Timestamp to convert
    :returns: Seconds since 00:00 monday

    """
    difference = stamp - week_start(stamp)
    return int(difference.total_seconds())


def week_seconds_to_datetime(seconds) -> datetime.datetime:
    """Return the datetime that is `seconds` from the start of this week.

    :param int seconds: Seconds
    :returns: Datetime for 00:00 Monday plus `seconds`

    """
    return week_start(datetime.datetime.now()) + datetime.timedelta(seconds=seconds)


def make_week_seconds(day, hour, minute=0, seconds=0) -> int:
    """Return :func:`week_seconds` for the given `day` of the week, `hour`
    and `minute`.

    :param int day: Zero-indexed day of the week
    :param int hour: Zero-indexed 24-hour
    :param int minute: Minute (default: ``0``)
    :param int seconds: Seconds (default: ``0``)
    :returns: Seconds since 00:00 Monday

    """
    stamp = week_start(datetime.datetime.now())
    stamp += datetime.timedelta(days=day)
    stamp = datetime.datetime.combine(
        stamp.date(), datetime.time(hour, minute, seconds)
    )
    return week_seconds(stamp)


def floor_minute(stamp=None) -> datetime.datetime:
    """Return `stamp` floored to the current minute. If no `stamp` is
    specified, the current time is used. Preserves timezone information.

    .. versionadded:: 2.0

    :param datetime stamp: `datetime` object to floor (default: now)
    :returns: Datetime floored to the minute

    """
    stamp = stamp - datetime.timedelta(
        seconds=stamp.second, microseconds=stamp.microsecond
    )
    return stamp


def floor_day(stamp=None):
    """Return `stamp` floored to the current day. If no `stamp` is specified,
    the current time is used. This is similar to the
    :meth:`~datetime.datetime.date` method, but returns a
    :class:`~datetime.datetime` object, instead of a
    :class:`~datetime.date` object.

    This is the same as :func:`~pytool.time.trim_time`.

    .. versionchanged:: 2.0
       Preserves timezone information if it exists, and uses
       :func:`pytool.time.utcnow` instead of :meth:`datetime.datetime.now`
       if `stamp` is not given.

    :param datetime stamp: `datetime` object to floor (default: now)
    :returns: Datetime floored to the day

    """
    stamp = stamp or utcnow()
    return trim_time(stamp)


def floor_week(stamp=None):
    """Return `stamp` floored to the current week, at 00:00 Monday. If no
    `stamp` is specified, the current time is used. Preserves timezone
    information.

    This is the same as :func:`~pytool.time.week_start`

    .. versionadded:: 2.0

    :param datetime stamp: `datetime` object to floor (default now:)
    :returns: Datetime floored to the week

    """
    stamp = stamp or utcnow()
    return week_start(stamp)


def floor_month(stamp=None):
    """Return `stamp` floored to the current month. If no `stamp` is specified,
    the current time is used.

    .. versionchanged:: 2.0
       Preserves timezone information if it exists, and uses
       :func:`pytool.time.utcnow` instead of :meth:`datetime.datetime.now`
       if `stamp` is not given.

    :param datetime stamp: `datetime` object to floor (default: now)
    :returns: Datetime floored to the month

    """
    stamp = stamp or utcnow()
    return datetime.datetime(stamp.year, stamp.month, 1, tzinfo=stamp.tzinfo)


def ago(stamp=None, **kwargs):
    """Return the current time as UTC minutes the specified timeframe.

    This is a helper for simplifying the common pattern of
    `pytool.time.utcnow() - datetime.timedelta(mintues=15)`.

    :param stamp: An optional timestamp instead of `utcnow()`
    :param days: Days previous
    :param hours: Hours previous
    :param minutes: Minutes previous
    :param seconds: Days previous
    :returns: UTC timestamp

    If you like brevity in your arguments, you can use the shorter
    versions, `hrs=`, `mins=` and `secs=`.

    Or if you really want a short signature, you can use `d=`, `h=`, `m=`,
    `s=`.

    ::

        import pytool

        yesterday = pytool.time.ago(days=1)

        a_little_while_ago = pytool.time.ago(minutes=1)

        a_little_before = pytool.time.ago(my_date, hours=1)

        # Shorter arguments
        shorter = pytool.time.ago(hrs=1, mins=1, secs=1)

        # Shorthand argument names
        short = pytool.time.ago(d=1, h=1, m=1, s=1)

    """
    _map = {
        "days": "days",
        "d": "days",
        "hours": "hours",
        "hrs": "hours",
        "h": "hours",
        "minutes": "minutes",
        "mins": "minutes",
        "m": "minutes",
        "seconds": "seconds",
        "secs": "seconds",
        "s": "seconds",
    }

    args = {}

    for key in kwargs:
        args[_map[key]] = kwargs[key]

    stamp = stamp or utcnow()
    stamp -= datetime.timedelta(**args)

    return stamp
