from __future__ import absolute_import
import six
import time
import datetime
import itertools


# There might be a better way to get the epoch with tzinfo, please create
# a pull request if you know a better way that functions for Python 2 and 3
epoch = datetime.datetime(year=1970, month=1, day=1)


def timedelta_to_seconds(delta):
    '''Convert a timedelta to seconds with the microseconds as fraction

    Note that this method has become largely obsolete with the
    `timedelta.total_seconds()` method introduced in Python 2.7.

    >>> from datetime import timedelta
    >>> '%d' % timedelta_to_seconds(timedelta(days=1))
    '86400'
    >>> '%d' % timedelta_to_seconds(timedelta(seconds=1))
    '1'
    >>> '%.6f' % timedelta_to_seconds(timedelta(seconds=1, microseconds=1))
    '1.000001'
    >>> '%.6f' % timedelta_to_seconds(timedelta(microseconds=1))
    '0.000001'
    '''
    # Only convert to float if needed
    if delta.microseconds:
        total = delta.microseconds * 1e-6
    else:
        total = 0
    total += delta.seconds
    total += delta.days * 60 * 60 * 24
    return total


def format_time(timestamp, precision=datetime.timedelta(seconds=1)):
    '''Formats timedelta/datetime/seconds

    >>> format_time('1')
    '0:00:01'
    >>> format_time(1.234)
    '0:00:01'
    >>> format_time(1)
    '0:00:01'
    >>> format_time(datetime.datetime(2000, 1, 2, 3, 4, 5, 6))
    '2000-01-02 03:04:05'
    >>> format_time(datetime.date(2000, 1, 2))
    '2000-01-02'
    >>> format_time(datetime.timedelta(seconds=3661))
    '1:01:01'
    >>> format_time(None)
    '--:--:--'
    >>> format_time(format_time)  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    TypeError: Unknown type ...

    '''
    precision_seconds = precision.total_seconds()

    if isinstance(timestamp, six.string_types + six.integer_types + (float, )):
        try:
            castfunc = six.integer_types[-1]
            timestamp = datetime.timedelta(seconds=castfunc(timestamp))
        except OverflowError:  # pragma: no cover
            timestamp = None

    if isinstance(timestamp, datetime.timedelta):
        seconds = timestamp.total_seconds()
        # Truncate the number to the given precision
        seconds = seconds - (seconds % precision_seconds)

        return str(datetime.timedelta(seconds=seconds))
    elif isinstance(timestamp, datetime.datetime):  # pragma: no cover
        # Python 2 doesn't have the timestamp method
        if hasattr(timestamp, 'timestamp'):
            seconds = timestamp.timestamp()
        else:
            seconds = timedelta_to_seconds(timestamp - epoch)

        # Truncate the number to the given precision
        seconds = seconds - (seconds % precision_seconds)

        try:  # pragma: no cover
            if six.PY3:
                dt = datetime.datetime.fromtimestamp(seconds)
            else:
                dt = datetime.datetime.utcfromtimestamp(seconds)
        except ValueError:  # pragma: no cover
            dt = datetime.datetime.max
        return str(dt)
    elif isinstance(timestamp, datetime.date):
        return str(timestamp)
    elif timestamp is None:
        return '--:--:--'
    else:
        raise TypeError('Unknown type %s: %r' % (type(timestamp), timestamp))


def timeout_generator(
    timeout,
    interval=datetime.timedelta(seconds=1),
    iterable=itertools.count,
    interval_multiplier=1.0,
):
    '''
    Generator that walks through the given iterable (a counter by default)
    until the timeout is reached with a configurable interval between items

    >>> for i in timeout_generator(0.1, 0.06):
    ...     print(i)
    0
    1
    2
    >>> timeout = datetime.timedelta(seconds=0.1)
    >>> interval = datetime.timedelta(seconds=0.06)
    >>> for i in timeout_generator(timeout, interval, itertools.count()):
    ...     print(i)
    0
    1
    2
    >>> for i in timeout_generator(1, interval=0.1, iterable='ab'):
    ...     print(i)
    a
    b

    >>> timeout = datetime.timedelta(seconds=0.1)
    >>> interval = datetime.timedelta(seconds=0.06)
    >>> for i in timeout_generator(timeout, interval, interval_multiplier=2):
    ...     print(i)
    0
    1
    '''

    if isinstance(interval, datetime.timedelta):
        interval = timedelta_to_seconds(interval)

    if isinstance(timeout, datetime.timedelta):
        timeout = timedelta_to_seconds(timeout)

    if callable(iterable):
        iterable = iterable()

    interval *= interval_multiplier
    time.sleep(interval)

    if six.PY3:  # pragma: no cover
        timer = time.perf_counter
    else:  # pragma: no cover
        timer = time.time

    end = timeout + timer()
    for item in iterable:
        yield item

        if timer() >= end:
            break

        interval *= interval_multiplier
        time.sleep(interval)
