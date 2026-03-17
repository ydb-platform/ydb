"""
This module provides utility functions for handling time-related operations.

Functions:
- timedelta_to_seconds: Convert a timedelta to seconds with microseconds as
  fraction.
- delta_to_seconds: Convert a timedelta or numeric interval to seconds.
- delta_to_seconds_or_none: Convert a timedelta to seconds or return None.
- format_time: Format a timestamp (timedelta, datetime, or seconds) to a
  string.
- timeout_generator: Generate items from an iterable until a timeout is
  reached.
- aio_timeout_generator: Asynchronously generate items from an iterable until a
  timeout is reached.
- aio_generator_timeout_detector: Detect if an async generator has not yielded
  an element for a set amount of time.
- aio_generator_timeout_detector_decorator: Decorator for
  aio_generator_timeout_detector.
"""

# pyright: reportUnnecessaryIsInstance=false
import asyncio
import datetime
import functools
import itertools
import time

import python_utils
from python_utils import aio, exceptions, types

_T = types.TypeVar('_T')
_P = types.ParamSpec('_P')


# There might be a better way to get the epoch with tzinfo, please create
# a pull request if you know a better way that functions for Python 2 and 3
epoch = datetime.datetime(year=1970, month=1, day=1)


def timedelta_to_seconds(delta: datetime.timedelta) -> types.Number:
    """Convert a timedelta to seconds with the microseconds as fraction.

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
    """
    # Only convert to float if needed
    if delta.microseconds:
        total = delta.microseconds * 1e-6
    else:
        total = 0
    total += delta.seconds
    total += delta.days * 60 * 60 * 24
    return total


def delta_to_seconds(interval: types.delta_type) -> types.Number:
    """
    Convert a timedelta to seconds.

    >>> delta_to_seconds(datetime.timedelta(seconds=1))
    1
    >>> delta_to_seconds(datetime.timedelta(seconds=1, microseconds=1))
    1.000001
    >>> delta_to_seconds(1)
    1
    >>> delta_to_seconds('whatever')  # doctest: +ELLIPSIS
    Traceback (most recent call last):
        ...
    TypeError: Unknown type ...
    """
    if isinstance(interval, datetime.timedelta):
        return timedelta_to_seconds(interval)
    elif isinstance(interval, (int, float)):
        return interval
    else:
        raise TypeError(f'Unknown type {type(interval)}: {interval!r}')


def delta_to_seconds_or_none(
    interval: types.Optional[types.delta_type],
) -> types.Optional[types.Number]:
    """Convert a timedelta to seconds or return None."""
    if interval is None:
        return None
    else:
        return delta_to_seconds(interval)


def format_time(
    timestamp: types.timestamp_type,
    precision: datetime.timedelta = datetime.timedelta(seconds=1),
) -> str:
    """Formats timedelta/datetime/seconds.

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

    """
    precision_seconds = precision.total_seconds()

    if isinstance(timestamp, str):
        timestamp = float(timestamp)

    if isinstance(timestamp, (int, float)):
        try:
            timestamp = datetime.timedelta(seconds=timestamp)
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
            dt = datetime.datetime.fromtimestamp(seconds)
        except (ValueError, OSError):  # pragma: no cover
            dt = datetime.datetime.max
        return str(dt)
    elif isinstance(timestamp, datetime.date):
        return str(timestamp)
    elif timestamp is None:
        return '--:--:--'
    else:
        raise TypeError(f'Unknown type {type(timestamp)}: {timestamp!r}')


@types.overload
def _to_iterable(
    iterable: types.Union[
        types.Callable[[], types.AsyncIterable[_T]],
        types.AsyncIterable[_T],
    ],
) -> types.AsyncIterable[_T]: ...


@types.overload
def _to_iterable(
    iterable: types.Union[
        types.Callable[[], types.Iterable[_T]], types.Iterable[_T]
    ],
) -> types.Iterable[_T]: ...


def _to_iterable(
    iterable: types.Union[
        types.Iterable[_T],
        types.Callable[[], types.Iterable[_T]],
        types.AsyncIterable[_T],
        types.Callable[[], types.AsyncIterable[_T]],
    ],
) -> types.Union[types.Iterable[_T], types.AsyncIterable[_T]]:
    if callable(iterable):
        return iterable()
    else:
        return iterable


def timeout_generator(
    timeout: types.delta_type,
    interval: types.delta_type = datetime.timedelta(seconds=1),
    iterable: types.Union[
        types.Iterable[_T], types.Callable[[], types.Iterable[_T]]
    ] = itertools.count,  # type: ignore[assignment]
    interval_multiplier: float = 1.0,
    maximum_interval: types.Optional[types.delta_type] = None,
) -> types.Iterable[_T]:
    """
    Generator that walks through the given iterable (a counter by default)
    until the float_timeout is reached with a configurable float_interval
    between items.

    This can be used to limit the time spent on a slow operation. This can be
    useful for testing slow APIs so you get a small sample of the data in a
    reasonable amount of time.

    >>> for i in timeout_generator(0.1, 0.06):
    ...     # Put your slow code here
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
    2
    """
    float_interval: float = delta_to_seconds(interval)
    float_maximum_interval: types.Optional[float] = delta_to_seconds_or_none(
        maximum_interval
    )
    iterable_ = _to_iterable(iterable)

    end = delta_to_seconds(timeout) + time.perf_counter()
    for item in iterable_:
        yield item

        if time.perf_counter() >= end:
            break

        time.sleep(float_interval)

        float_interval *= interval_multiplier
        if float_maximum_interval:
            float_interval = min(float_interval, float_maximum_interval)


async def aio_timeout_generator(
    timeout: types.delta_type,  # noqa: ASYNC109
    interval: types.delta_type = datetime.timedelta(seconds=1),
    iterable: types.Union[
        types.AsyncIterable[_T], types.Callable[..., types.AsyncIterable[_T]]
    ] = aio.acount,
    interval_multiplier: float = 1.0,
    maximum_interval: types.Optional[types.delta_type] = None,
) -> types.AsyncGenerator[_T, None]:
    """
    Async generator that walks through the given async iterable (a counter by
    default) until the float_timeout is reached with a configurable
    float_interval between items.

    The interval_exponent automatically increases the float_timeout with each
    run. Note that if the float_interval is less than 1, 1/interval_exponent
    will be used so the float_interval is always growing. To double the
    float_interval with each run, specify 2.

    Doctests and asyncio are not friends, so no examples. But this function is
    effectively the same as the `timeout_generator` but it uses `async for`
    instead.
    """
    float_interval: float = delta_to_seconds(interval)
    float_maximum_interval: types.Optional[float] = delta_to_seconds_or_none(
        maximum_interval
    )
    iterable_ = _to_iterable(iterable)

    end = delta_to_seconds(timeout) + time.perf_counter()
    async for item in iterable_:  # pragma: no branch
        yield item

        if time.perf_counter() >= end:
            break

        await asyncio.sleep(float_interval)

        float_interval *= interval_multiplier
        if float_maximum_interval:  # pragma: no branch
            float_interval = min(float_interval, float_maximum_interval)


async def aio_generator_timeout_detector(
    generator: types.AsyncGenerator[_T, None],
    timeout: types.Optional[types.delta_type] = None,  # noqa: ASYNC109
    total_timeout: types.Optional[types.delta_type] = None,
    on_timeout: types.Optional[
        types.Callable[
            [
                types.AsyncGenerator[_T, None],
                types.Optional[types.delta_type],
                types.Optional[types.delta_type],
                BaseException,
            ],
            types.Any,
        ]
    ] = exceptions.reraise,
    **on_timeout_kwargs: types.Mapping[types.Text, types.Any],
) -> types.AsyncGenerator[_T, None]:
    """
    This function is used to detect if an asyncio generator has not yielded
    an element for a set amount of time.

    The `on_timeout` argument is called with the `generator`, `timeout`,
    `total_timeout`, `exception` and the extra `**kwargs` to this function as
    arguments.
    If `on_timeout` is not specified, the exception is reraised.
    If `on_timeout` is `None`, the exception is silently ignored and the
    generator will finish as normal.
    """
    if total_timeout is None:
        total_timeout_end = None
    else:
        total_timeout_end = time.perf_counter() + delta_to_seconds(
            total_timeout
        )

    timeout_s = python_utils.delta_to_seconds_or_none(timeout)

    while True:
        try:
            if total_timeout_end and time.perf_counter() >= total_timeout_end:
                raise asyncio.TimeoutError(  # noqa: TRY301
                    'Total timeout reached'
                )

            if timeout_s:
                yield await asyncio.wait_for(generator.__anext__(), timeout_s)
            else:
                yield await generator.__anext__()

        except asyncio.TimeoutError as exception:  # noqa: PERF203
            if on_timeout is not None:
                await on_timeout(
                    generator,
                    timeout,
                    total_timeout,
                    exception,
                    **on_timeout_kwargs,
                )
            break

        except StopAsyncIteration:
            break


def aio_generator_timeout_detector_decorator(
    timeout: types.Optional[types.delta_type] = None,
    total_timeout: types.Optional[types.delta_type] = None,
    on_timeout: types.Optional[
        types.Callable[
            [
                types.AsyncGenerator[types.Any, None],
                types.Optional[types.delta_type],
                types.Optional[types.delta_type],
                BaseException,
            ],
            types.Any,
        ]
    ] = exceptions.reraise,
    **on_timeout_kwargs: types.Mapping[types.Text, types.Any],
) -> types.Callable[
    [types.Callable[_P, types.AsyncGenerator[_T, None]]],
    types.Callable[_P, types.AsyncGenerator[_T, None]],
]:
    """A decorator wrapper for aio_generator_timeout_detector."""

    def _timeout_detector_decorator(
        generator: types.Callable[_P, types.AsyncGenerator[_T, None]],
    ) -> types.Callable[_P, types.AsyncGenerator[_T, None]]:
        """The decorator itself."""

        @functools.wraps(generator)
        def wrapper(
            *args: _P.args,
            **kwargs: _P.kwargs,
        ) -> types.AsyncGenerator[_T, None]:
            return aio_generator_timeout_detector(
                generator(*args, **kwargs),
                timeout,
                total_timeout,
                on_timeout,
                **on_timeout_kwargs,
            )

        return wrapper

    return _timeout_detector_decorator
