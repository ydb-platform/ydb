# Copyright (c) "Neo4j"
# Neo4j Sweden AB [https://neo4j.com]
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


from datetime import (
    datetime,
    time,
    timezone,
)

from ...._optional_deps import (
    np,
    pd,
)
from ....time import (
    _NANO_SECONDS,
    Date,
    DateTime,
    Duration,
    MAX_YEAR,
    MIN_YEAR,
    Time,
)
from ...packstream import Structure


ANY_BUILTIN_DATETIME = datetime(1970, 1, 1)


def get_date_unix_epoch():
    return Date(1970, 1, 1)


def get_date_unix_epoch_ordinal():
    return get_date_unix_epoch().to_ordinal()


def get_datetime_unix_epoch_utc():
    from pytz import utc

    return DateTime(1970, 1, 1, 0, 0, 0, utc)


def hydrate_date(days):
    """
    Hydrator for ``Date`` values.

    :param days:
    :returns: Date
    """
    return Date.from_ordinal(get_date_unix_epoch_ordinal() + days)


def dehydrate_date(value):
    """
    Dehydrator for ``date`` values.

    :param value:
    :type value: Date
    :returns:
    """
    return Structure(
        b"D", value.toordinal() - get_date_unix_epoch().toordinal()
    )


def hydrate_time(nanoseconds, tz=None):
    """
    Hydrator for ``Time`` and ``LocalTime`` values.

    :param nanoseconds:
    :param tz:
    :returns: Time
    """
    from pytz import FixedOffset

    seconds, nanoseconds = map(int, divmod(nanoseconds, 1000000000))
    minutes, seconds = map(int, divmod(seconds, 60))
    hours, minutes = map(int, divmod(minutes, 60))
    t = Time(hours, minutes, seconds, nanoseconds)
    if tz is None:
        return t
    tz_offset_minutes, _tz_offset_seconds = divmod(tz, 60)
    zone = FixedOffset(tz_offset_minutes)
    return zone.localize(t)


def dehydrate_time(value):
    """
    Dehydrator for ``time`` values.

    :param value:
    :type value: Time
    :returns:
    """
    if isinstance(value, Time):
        nanoseconds = value.ticks
    elif isinstance(value, time):
        nanoseconds = (
            3600000000000 * value.hour
            + 60000000000 * value.minute
            + 1000000000 * value.second
            + 1000 * value.microsecond
        )
    else:
        raise TypeError("Value must be a neo4j.time.Time or a datetime.time")
    if value.tzinfo:
        return Structure(
            b"T",
            nanoseconds,
            int(value.tzinfo.utcoffset(value).total_seconds()),
        )
    else:
        return Structure(b"t", nanoseconds)


def hydrate_datetime(seconds, nanoseconds, tz=None):
    """
    Hydrator for ``DateTime`` and ``LocalDateTime`` values.

    :param seconds:
    :param nanoseconds:
    :param tz:
    :returns: datetime
    """
    from pytz import (
        FixedOffset,
        timezone,
    )

    minutes, seconds = map(int, divmod(seconds, 60))
    hours, minutes = map(int, divmod(minutes, 60))
    days, hours = map(int, divmod(hours, 24))
    t = DateTime.combine(
        Date.from_ordinal(get_date_unix_epoch_ordinal() + days),
        Time(hours, minutes, seconds, nanoseconds),
    )
    if tz is None:
        return t
    if isinstance(tz, int):
        tz_offset_minutes, _tz_offset_seconds = divmod(tz, 60)
        zone = FixedOffset(tz_offset_minutes)
    else:
        zone = timezone(tz)
    return zone.localize(t)


def dehydrate_datetime(value):
    """
    Dehydrator for ``datetime`` values.

    :param value:
    :type value: datetime or DateTime
    :returns:
    """

    def seconds_and_nanoseconds(dt):
        if isinstance(dt, datetime):
            dt = DateTime.from_native(dt)
        zone_epoch = DateTime(1970, 1, 1, tzinfo=dt.tzinfo)
        dt_clock_time = dt._to_clock_time()
        zone_epoch_clock_time = zone_epoch._to_clock_time()
        t = dt_clock_time - zone_epoch_clock_time
        return t.seconds, t.nanoseconds

    tz = value.tzinfo
    if tz is None:
        # without time zone
        from pytz import utc

        value = utc.localize(value)
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"d", seconds, nanoseconds)
    elif hasattr(tz, "zone") and tz.zone and isinstance(tz.zone, str):
        # with named pytz time zone
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"f", seconds, nanoseconds, tz.zone)
    elif hasattr(tz, "key") and tz.key and isinstance(tz.key, str):
        # with named zoneinfo (Python 3.9+) time zone
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(b"f", seconds, nanoseconds, tz.key)
    else:
        if isinstance(tz, timezone):
            # offset of the timezone is constant, so any date will do
            offset = tz.utcoffset(ANY_BUILTIN_DATETIME)
        else:
            offset = tz.utcoffset(value)
        # with time offset
        seconds, nanoseconds = seconds_and_nanoseconds(value)
        return Structure(
            b"F", seconds, nanoseconds, int(offset.total_seconds())
        )


if np is not None:

    def dehydrate_np_datetime(value):
        """
        Dehydrator for ``numpy.datetime64`` values.

        :param value:
        :type value: numpy.datetime64
        :returns:
        """
        if np.isnat(value):
            return None
        year = value.astype("datetime64[Y]").astype(np.int64) + 1970
        if not 0 < year <= 9999:
            # while we could encode years outside the range, they would fail
            # when retrieved from the database.
            raise ValueError(
                f"Year out of range ({MIN_YEAR:d}..{MAX_YEAR:d}) found {year}"
            )
        seconds = value.astype(np.dtype("datetime64[s]")).astype(np.int64)
        nanoseconds = (
            value.astype(np.dtype("datetime64[ns]")).astype(np.int64)
            % _NANO_SECONDS
        )
        return Structure(b"d", seconds, nanoseconds)


if pd is not None:

    def dehydrate_pandas_datetime(value):
        """
        Dehydrator for ``pandas.Timestamp`` values.

        :param value:
        :type value: pandas.Timestamp
        :returns:
        """
        return dehydrate_datetime(
            DateTime(
                value.year,
                value.month,
                value.day,
                value.hour,
                value.minute,
                value.second,
                value.microsecond * 1000 + value.nanosecond,
                value.tzinfo,
            )
        )


def hydrate_duration(months, days, seconds, nanoseconds):
    """
    Hydrator for ``Duration`` values.

    :param months:
    :param days:
    :param seconds:
    :param nanoseconds:
    :returns: ``duration`` namedtuple
    """
    return Duration(
        months=months, days=days, seconds=seconds, nanoseconds=nanoseconds
    )


def dehydrate_duration(value):
    """
    Dehydrator for ``duration`` values.

    :param value:
    :type value: Duration
    :returns:
    """
    return Structure(
        b"E", value.months, value.days, value.seconds, value.nanoseconds
    )


def dehydrate_timedelta(value):
    """
    Dehydrator for ``timedelta`` values.

    :param value:
    :type value: timedelta
    :returns:
    """
    months = 0
    days = value.days
    seconds = value.seconds
    nanoseconds = 1000 * value.microseconds
    return Structure(b"E", months, days, seconds, nanoseconds)


if np is not None:
    _NUMPY_DURATION_NS_FALLBACK = object()
    _NUMPY_DURATION_UNITS = {
        "Y": "years",
        "M": "months",
        "W": "weeks",
        "D": "days",
        "h": "hours",
        "m": "minutes",
        "s": "seconds",
        "ms": "milliseconds",
        "us": "microseconds",
        "ns": "nanoseconds",
        "ps": _NUMPY_DURATION_NS_FALLBACK,
        "fs": _NUMPY_DURATION_NS_FALLBACK,
        "as": _NUMPY_DURATION_NS_FALLBACK,
    }

    def dehydrate_np_timedelta(value):
        """
        Dehydrator for ``numpy.timedelta64`` values.

        :param value:
        :type value: numpy.timedelta64
        :returns:
        """
        if np.isnat(value):
            return None
        unit, step_size = np.datetime_data(value)
        numer = int(value.astype(np.int64))
        try:
            kwarg = _NUMPY_DURATION_UNITS[unit]
        except KeyError:
            raise TypeError(
                f"Unsupported numpy.timedelta64 unit: {unit!r}"
            ) from None
        if kwarg is _NUMPY_DURATION_NS_FALLBACK:
            nanoseconds = value.astype("timedelta64[ns]").astype(np.int64)
            return dehydrate_duration(Duration(nanoseconds=nanoseconds))
        return dehydrate_duration(Duration(**{kwarg: numer * step_size}))


if pd is not None:

    def dehydrate_pandas_timedelta(value):
        """
        Dehydrator for ``pandas.Timedelta`` values.

        :param value:
        :type value: pandas.Timedelta
        :returns:
        """
        return dehydrate_duration(Duration(nanoseconds=value.value))
