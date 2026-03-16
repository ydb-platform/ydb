import re
from datetime import datetime, timezone


def generate(
    dt: datetime,
    utc: bool = True,
    accept_naive: bool = False,
    microseconds: bool = False,
) -> str:
    """
    Generate an :RFC:`3339`-formatted timestamp from a :class:`datetime.datetime`.

    >>> from datetime import datetime, timezone
    >>> from zoneinfo import ZoneInfo
    >>> generate(datetime(2009, 1, 1, 12, 59, 59, 0, timezone.utc))
    '2009-01-01T12:59:59Z'

    The timestamp be normalized to UTC unless :python:`utc=False` is specified, in which case
    it will use the timezone from the :class:`~datetime.datetime`'s :attr:`~datetime.datetime.tzinfo` attribute.

    >>> eastern = ZoneInfo('US/Eastern')
    >>> dt = datetime(2009, 1, 1, 12, 59, 59, tzinfo=eastern)
    >>> generate(dt)
    '2009-01-01T17:59:59Z'
    >>> generate(dt, utc=False)
    '2009-01-01T12:59:59-05:00'

    Unless :python:`accept_naive=True` is specified, the :class:`~datetime.datetime` must not be naive.

    >>> generate(datetime(2009, 1, 1, 12, 59, 59, 0))
    Traceback (most recent call last):
    ...
    ValueError: naive datetime and accept_naive is False

    >>> generate(datetime(2009, 1, 1, 12, 59, 59, 0), accept_naive=True)
    '2009-01-01T12:59:59Z'

    If, however, :python:`accept_naive=True` is specified, the :class:`~datetime.datetime` is assumed to represent a UTC time.
    Attempting to generate a local timestamp from a naive datetime will result in an error.

    >>> generate(datetime(2009, 1, 1, 12, 59, 59, 0), accept_naive=True, utc=False)
    Traceback (most recent call last):
    ...
    ValueError: cannot generate a local timestamp from a naive datetime

    :param datetime.datetime dt: the :class:`~datetime.datetime` for which to generate an :RFC:`3339` timestamp.
    :param bool utc: :const:`True` to normalize the supplied :class:`datetime.datetime` to UTC; :const:`False` otherwise.
                     Defaults to :const:`True`.
    :param bool accept_naive: :const:`True` if :func:`generate()` should accept a 'naive' datetime
                              (that is, one without timezone information) and treat it as a UTC timestamp;
                              :const:`False` otherwise. Defaults to :const:`False`.
    :param bool microseconds: :const:`True` to generate a timestamp which includes fractional seconds, if present;
                              :const:`False` otherwise. Defaults to :const:`False`.
                              Note that fractional seconds are *truncated*,
                              not rounded when :obj:`microseconds` is :const:`False`.
    :return: the supplied :class:`~datetime.datetime` instance represented as an :RFC:`3339` timestamp
    :rtype: str

    """

    if dt.tzinfo is None:
        if accept_naive:
            if utc:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                raise ValueError(
                    "cannot generate a local timestamp from a naive datetime"
                )
        else:
            raise ValueError("naive datetime and accept_naive is False")

    if utc:
        dt = dt.astimezone(timezone.utc)

    timestamp = dt.isoformat(timespec="microseconds" if microseconds else "seconds")

    if dt.tzinfo == timezone.utc:
        timestamp = re.sub(r"\+00:00$", "Z", timestamp)

    return timestamp
