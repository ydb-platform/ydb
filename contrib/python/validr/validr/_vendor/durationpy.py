"""
https://github.com/icholy/durationpy/blob/master/durationpy/duration.py

>>> int(from_str('0').total_seconds())
0
>>> int(from_str('60s').total_seconds())
60
>>> int(from_str('3m30s').total_seconds())
210
>>> int(from_str('2h3m30s').total_seconds())
7410
>>> int(from_str('1d10s').total_seconds())
86410
>>> int(from_str('-1s').total_seconds())
-1
>>> print(to_str(datetime.timedelta(seconds=5)))
5s
>>> print(to_str(datetime.timedelta(seconds=59)))
59s
>>> print(to_str(datetime.timedelta(seconds=61)))
1m1s
>>> print(to_str(datetime.timedelta(seconds=3600)))
1h
>>> print(to_str(datetime.timedelta(seconds=3600 + 60 + 5)))
1h1m5s
>>> print(to_str(datetime.timedelta(seconds=23*60*60 + 60 + 5)))
23h1m5s
>>> print(to_str(datetime.timedelta(seconds=0.005)))
5ms
>>> print(to_str(datetime.timedelta(seconds=0.000005)))
5us
>>> D = 24*60*60
>>> print(to_str(datetime.timedelta(seconds=2*D + 60 + 5), extended=True))
2d1m5s
>>> print(to_str(datetime.timedelta(seconds=35*D + 60 + 5), extended=True))
1mo5d1m5s
>>> print(to_str(datetime.timedelta(seconds=370*D + 60 + 5), extended=True))
1y5d1m5s
>>> from_str('10x')
Traceback (most recent call last):
...
validr._vendor.durationpy.DurationError: Unknown unit x in duration 10x
>>> from_str('1..0h')
Traceback (most recent call last):
...
validr._vendor.durationpy.DurationError: Invalid value 1..0 in duration 1..0h
"""
import re
import datetime

_nanosecond_size = 1
_microsecond_size = 1000 * _nanosecond_size
_millisecond_size = 1000 * _microsecond_size
_second_size = 1000 * _millisecond_size
_minute_size = 60 * _second_size
_hour_size = 60 * _minute_size
_day_size = 24 * _hour_size
_week_size = 7 * _day_size
_month_size = 30 * _day_size
_year_size = 365 * _day_size

_units = {
    "ns": _nanosecond_size,
    "us": _microsecond_size,
    "µs": _microsecond_size,
    "μs": _microsecond_size,
    "ms": _millisecond_size,
    "s": _second_size,
    "m": _minute_size,
    "h": _hour_size,
    "d": _day_size,
    "w": _week_size,
    "mm": _month_size,  # deprecated, use 'mo' instead
    "mo": _month_size,
    "y": _year_size,
}


class DurationError(ValueError):
    """duration error"""


_RE_DURATION = re.compile(r'([\d\.]+)([a-zµμ]+)')


def from_str(duration: str) -> datetime.timedelta:
    """Parse a duration string to a datetime.timedelta"""

    duration = duration.strip()

    if duration in ("0", "+0", "-0"):
        return datetime.timedelta()

    total = 0
    sign = -1 if duration[0] == '-' else 1
    matches = _RE_DURATION.findall(duration)

    if not len(matches):
        raise DurationError("Invalid duration {}".format(duration))

    for (value, unit) in matches:
        if unit not in _units:
            err_msg = "Unknown unit {} in duration {}".format(unit, duration)
            raise DurationError(err_msg)
        try:
            total += float(value) * _units[unit]
        except Exception:
            err_msg = "Invalid value {} in duration {}".format(value, duration)
            raise DurationError(err_msg)

    microseconds = total / _microsecond_size
    return datetime.timedelta(microseconds=sign * microseconds)


def to_str(delta: datetime.timedelta, extended=False) -> str:
    """Format a datetime.timedelta to a duration string"""

    total_seconds = delta.total_seconds()
    sign = "-" if total_seconds < 0 else ""
    nanoseconds = abs(total_seconds * _second_size)

    if total_seconds < 1:
        result_str = _to_str_small(nanoseconds, extended)
    else:
        result_str = _to_str_large(nanoseconds, extended)

    return "{}{}".format(sign, result_str)


def _to_str_small(nanoseconds, extended):

    result_str = ""

    if not nanoseconds:
        return "0"

    milliseconds = int(nanoseconds / _millisecond_size)
    if milliseconds:
        nanoseconds -= _millisecond_size * milliseconds
        result_str += "{:g}ms".format(milliseconds)

    microseconds = int(nanoseconds / _microsecond_size)
    if microseconds:
        nanoseconds -= _microsecond_size * microseconds
        result_str += "{:g}us".format(microseconds)

    if nanoseconds:
        result_str += "{:g}ns".format(nanoseconds)

    return result_str


def _to_str_large(nanoseconds, extended):

    result_str = ""

    if extended:

        years = int(nanoseconds / _year_size)
        if years:
            nanoseconds -= _year_size * years
            result_str += "{:g}y".format(years)

        months = int(nanoseconds / _month_size)
        if months:
            nanoseconds -= _month_size * months
            result_str += "{:g}mo".format(months)

        days = int(nanoseconds / _day_size)
        if days:
            nanoseconds -= _day_size * days
            result_str += "{:g}d".format(days)

    hours = int(nanoseconds / _hour_size)
    if hours:
        nanoseconds -= _hour_size * hours
        result_str += "{:g}h".format(hours)

    minutes = int(nanoseconds / _minute_size)
    if minutes:
        nanoseconds -= _minute_size * minutes
        result_str += "{:g}m".format(minutes)

    seconds = float(nanoseconds) / float(_second_size)
    if seconds:
        nanoseconds -= _second_size * seconds
        result_str += "{:g}s".format(seconds)

    return result_str
