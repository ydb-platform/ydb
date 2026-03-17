from datetime import datetime


def datetime_utcoffset(dt: datetime) -> float:
    """
    Return the UTC offset for an aware :class:`datetime.datetime` in seconds.

    >>> from datetime import datetime
    >>> from zoneinfo import ZoneInfo
    >>> z = ZoneInfo('US/Eastern')
    >>> dt = datetime(2024, 11, 5, 19, 7, 6, tzinfo=z)
    >>> datetime_utcoffset(dt)
    -18000.0

    >>> dt = datetime(2024, 11, 5, 19, 7, 6)
    >>> datetime_utcoffset(dt)
    Traceback (most recent call last):
    ...
    AssertionError

    :param datetime.datetime dt: a :class:`~datetime.datetime` instance; must be aware (that is, have a timezone attached)
    :return: the UTC offset of the supplied :class:`~datetime.datetime` in seconds
    :rtype: float

    """

    assert dt.tzinfo is not None

    tz = dt.tzinfo
    offset = tz.utcoffset(dt)

    assert offset is not None

    return offset.total_seconds()
