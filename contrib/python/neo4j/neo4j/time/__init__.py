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


"""
Fundamental temporal types for interchange with the DBMS.

This module contains the fundamental types used for temporal accounting as well
as a number of utility functions.
"""

from __future__ import annotations as _

import re as _re
from datetime import (
    date as _date,
    datetime as _datetime,
    time as _time,
    timedelta as _timedelta,
    timezone as _timezone,
    tzinfo as _tzinfo,
)
from functools import total_ordering as _total_ordering
from re import compile as _re_compile
from time import (
    gmtime as _gmtime,
    mktime as _mktime,
    struct_time as _struct_time,
)

from .. import _typing as _t
from .._warnings import deprecation_warn as _deprecation_warn
from ._arithmetic import (
    round_half_to_even as _round_half_to_even,
    symmetric_divmod as _symmetric_divmod,
)
from ._metaclasses import (
    DateTimeType as _DateTimeType,
    DateType as _DateType,
    TimeType as _TimeType,
)


if _t.TYPE_CHECKING:
    from typing_extensions import deprecated as _deprecated
else:
    from .._warnings import deprecated as _deprecated


if False:
    # Ugly work-around to make sphinx understand `@_t.overload`
    import typing as _t  # type: ignore[no-redef]


__all__ = [
    "MAX_INT64",
    "MAX_YEAR",
    "MIN_INT64",
    "MIN_YEAR",
    "Date",
    "DateTime",
    "Duration",
    "Midday",
    "Midnight",
    "Never",
    "Time",
    "UnixEpoch",
    "ZeroDate",
]


MIN_INT64: _t.Final[int] = -(2**63)
MAX_INT64: _t.Final[int] = (2**63) - 1

#: The smallest year number allowed in a :class:`.Date` or :class:`.DateTime`
#: object to be compatible with :class:`datetime.date` and
#: :class:`datetime.datetime`.
MIN_YEAR: _t.Final[int] = 1

#: The largest year number allowed in a :class:`.Date` or :class:`.DateTime`
#: object to be compatible with :class:`datetime.date` and
#: :class:`datetime.datetime`.
MAX_YEAR: _t.Final[int] = 9999

_DATE_ISO_PATTERN: _t.Final[_re.Pattern] = _re_compile(
    r"^(\d{4})-(\d{2})-(\d{2})$"
)
_TIME_ISO_PATTERN: _t.Final[_re.Pattern] = _re_compile(
    r"^(\d{2})(:(\d{2})(:((\d{2})"
    r"(\.\d*)?))?)?(([+-])(\d{2}):(\d{2})(:((\d{2})(\.\d*)?))?)?$"
)
_DURATION_ISO_PATTERN: _t.Final[_re.Pattern] = _re_compile(
    r"^P((\d+)Y)?((\d+)M)?((\d+)D)?"
    r"(T((\d+)H)?((\d+)M)?(((\d+)(\.\d+)?)?S)?)?$"
)

_NANO_SECONDS: _t.Final[int] = 1000000000
_AVERAGE_SECONDS_IN_MONTH: _t.Final[int] = 2629746
_AVERAGE_SECONDS_IN_DAY: _t.Final[int] = 86400

_FORMAT_F_REPLACE: _t.Final[_re.Pattern] = _re.compile(r"(?<!%)%f")


def _is_leap_year(year):
    if year % 4 != 0:
        return False
    if year % 100 != 0:
        return True
    return year % 400 == 0


_IS_LEAP_YEAR = {
    year: _is_leap_year(year) for year in range(MIN_YEAR, MAX_YEAR + 1)
}


def _days_in_year(year):
    return 366 if _IS_LEAP_YEAR[year] else 365


_DAYS_IN_YEAR = {
    year: _days_in_year(year) for year in range(MIN_YEAR, MAX_YEAR + 1)
}


def _days_in_month(year, month):
    if month in {9, 4, 6, 11}:
        return 30
    elif month != 2:
        return 31
    else:
        return 29 if _IS_LEAP_YEAR[year] else 28


_DAYS_IN_MONTH = {
    (year, month): _days_in_month(year, month)
    for year in range(MIN_YEAR, MAX_YEAR + 1)
    for month in range(1, 13)
}


def _normalize_day(year, month, day):
    """
    Coerce the day of the month to an internal value.

    That value may or may not match the "public" value.

    Except for the last three days of every month, all days are stored as-is.
    The last three days are instead stored as -1 (the last), -2 (the second to
    last) and -3 (the third to last).

    Therefore, for a 28-day month, the last week is as follows:

        Day   | 22 23 24 25 26 27 28
        Value | 22 23 24 25 -3 -2 -1

    For a 29-day month, the last week is as follows:

        Day   | 23 24 25 26 27 28 29
        Value | 23 24 25 26 -3 -2 -1

    For a 30-day month, the last week is as follows:

        Day   | 24 25 26 27 28 29 30
        Value | 24 25 26 27 -3 -2 -1

    For a 31-day month, the last week is as follows:

        Day   | 25 26 27 28 29 30 31
        Value | 25 26 27 28 -3 -2 -1

    This slightly unintuitive system makes some temporal arithmetic
    produce a more desirable outcome.

    :param year:
    :param month:
    :param day:
    :returns:
    """
    if year < MIN_YEAR or year > MAX_YEAR:
        raise ValueError(f"Year out of range ({MIN_YEAR}..{MAX_YEAR})")
    if month < 1 or month > 12:
        raise ValueError("Month out of range (1..12)")
    days_in_month = _DAYS_IN_MONTH[year, month]
    if day in {days_in_month, -1}:
        return year, month, -1
    if day in {days_in_month - 1, -2}:
        return year, month, -2
    if day in {days_in_month - 2, -3}:
        return year, month, -3
    if 1 <= day <= days_in_month - 3:
        return year, month, int(day)
    # TODO improve this error message
    raise ValueError(
        f"Day {day} out of range (1..{days_in_month}, -1, -2 ,-3)"
    )


# TODO: 7.0 - make private
class ClockTime(tuple):
    """
    A count of ``seconds`` and ``nanoseconds``.

    This class can be used to mark a particular point in time, relative to an
    externally-specified epoch.

    The ``seconds`` and ``nanoseconds`` values provided to the constructor can
    can have any sign but will be normalized internally into a positive or
    negative ``seconds`` value along with a positive ``nanoseconds`` value
    between ``0`` and ``999,999,999``. Therefore, ``ClockTime(-1, -1)`` is
    normalized to ``ClockTime(-2, 999999999)``.

    Note that the structure of a :class:`.ClockTime` object is similar to
    the ``timespec`` struct in C.

    .. deprecated:: 6.0
        :class:`ClockTime` is an implementation detail.
        It and its related methods will be removed in a future version.
    """

    def __new__(cls, seconds: float = 0, nanoseconds: int = 0) -> ClockTime:
        seconds, nanoseconds = divmod(
            int(_NANO_SECONDS * seconds) + int(nanoseconds), _NANO_SECONDS
        )
        return tuple.__new__(cls, (seconds, nanoseconds))

    def __add__(self, other):
        if isinstance(other, (int, float)):
            other = _ClockTime(other)
        if isinstance(other, _ClockTime):
            return _ClockTime(
                self.seconds + other.seconds,
                self.nanoseconds + other.nanoseconds,
            )
        if isinstance(other, Duration):
            if other.months or other.days:
                raise ValueError("Cannot add Duration with months or days")
            return _ClockTime(
                self.seconds + other.seconds,
                self.nanoseconds + int(other.nanoseconds),
            )
        return NotImplemented

    def __sub__(self, other):
        if isinstance(other, (int, float)):
            other = _ClockTime(other)
        if isinstance(other, _ClockTime):
            return _ClockTime(
                self.seconds - other.seconds,
                self.nanoseconds - other.nanoseconds,
            )
        if isinstance(other, Duration):
            if other.months or other.days:
                raise ValueError(
                    "Cannot subtract Duration with months or days"
                )
            return _ClockTime(
                self.seconds - other.seconds,
                self.nanoseconds - int(other.nanoseconds),
            )
        return NotImplemented

    def __repr__(self):
        s, ns = self
        return f"neo4j.time.ClockTime(seconds={s!r}, nanoseconds={ns!r})"

    @property
    def seconds(self):
        return self[0]

    @property
    def nanoseconds(self):
        return self[1]


class _Clock:
    """
    Accessor for time values.

    This class is fulfilled by implementations
    that subclass :class:`.Clock`. These implementations are contained within
    the ``neo4j.time._clock_implementations`` module, and are not intended to
    be accessed directly.

    Creating a new :class:`.Clock` instance will produce the highest
    precision clock implementation available.

        >>> clock = _Clock()
        >>> type(clock)                                        # doctest: +SKIP
        neo4j.time.clock_implementations.LibCClock
        >>> clock.local_time()                                 # doctest: +SKIP
        ClockTime(seconds=1525265942, nanoseconds=506844026)
    """

    __implementations = None

    def __new__(cls):
        if cls.__implementations is None:
            # Find an available clock with the best precision
            from . import (  # noqa: F401 needed to make subclasses available
                _clock_implementations,
            )

            cls.__implementations = sorted(
                (
                    clock
                    for clock in _Clock.__subclasses__()
                    if clock.available()
                ),
                key=lambda clock: clock.precision(),
                reverse=True,
            )
        if not cls.__implementations:
            raise RuntimeError("No clock implementations available")
        return object.__new__(cls.__implementations[0])

    @classmethod
    def precision(cls):
        """
        Return precision (decimal places) of this clock implementation.

        The precision of this clock implementation, represented as a
        number of decimal places. Therefore, for a nanosecond precision
        clock, this function returns ``9``.
        """
        raise NotImplementedError("No clock implementation selected")

    @classmethod
    def available(cls):
        """
        Indicate whether this clock implementation is available.

        A boolean flag to indicate whether this clock implementation is
        available on this platform.
        """
        raise NotImplementedError("No clock implementation selected")

    @classmethod
    def local_offset(cls):
        """
        Get the UTC offset of the local time.

        The offset from UTC for local time read from this clock.
        This may raise OverflowError if not supported, because of platform
        dependent C libraries.

        :returns:
        :rtype:

        :raises OverflowError:
        """
        # Adding and subtracting two days to avoid passing a pre-epoch time to
        # ``mktime``, which can cause a ``OverflowError`` on some platforms
        # (e.g., Windows).
        return _ClockTime(-int(_mktime(_gmtime(172800))) + 172800)

    def local_time(self):
        """
        Get the current local time relative to the Unix Epoch.

        Read and return the current local time from this clock, measured
        relative to the Unix Epoch. This may raise OverflowError if not
        supported, because of platform dependent C libraries.

        :returns:
        :rtype:

        :raises OverflowError:
        """
        return self.utc_time() + self.local_offset()

    def utc_time(self):
        """
        Read and return the current UTC time from this clock.

        Time is measured relative to the Unix Epoch.
        """
        raise NotImplementedError("No clock implementation selected")


if _t.TYPE_CHECKING:
    # make typechecker believe that Duration subclasses datetime.timedelta
    # https://github.com/python/typeshed/issues/8409#issuecomment-1197704527
    _duration_base_class = _timedelta
else:
    _duration_base_class = object


class Duration(  # type: ignore[misc]
    tuple[int, int, int, int], _duration_base_class
):
    r"""
    A difference between two points in time.

    A :class:`.Duration` represents the difference between two points in
    time. Duration objects store a composite value of ``months``, ``days``,
    ``seconds``, and ``nanoseconds``. Unlike :class:`datetime.timedelta`
    however, days, and seconds/nanoseconds are never interchanged. All values
    except seconds and nanoseconds are applied separately in calculations
    (element-wise).

    A :class:`.Duration` stores four primary instance attributes internally:
    ``months``, ``days``, ``seconds`` and ``nanoseconds``. These are maintained
    as individual values and are immutable. Each of these four attributes can
    carry its own sign, except for ``nanoseconds``, which always has the same
    sign as ``seconds``. The constructor will establish this state, should the
    duration be initialized with conflicting ``seconds`` and ``nanoseconds``
    signs. This structure allows the modelling of durations such as
    ``3 months minus 2 days``.

    To determine if a :class:`Duration` ``d`` is overflowing the accepted
    values of the database, first, all ``nanoseconds`` outside the range
    -999_999_999 and 999_999_999 are transferred into the seconds field. Then,
    ``months``, ``days``, and ``seconds`` are summed up like so:
    ``months * 2629746 + days * 86400 + d.seconds
    + d.nanoseconds // 1000000000``.
    (Like the integer division in Python, this one is to be understood as
    rounding down rather than towards 0.)
    This value must be between -(2\ :sup:`63`) and (2\ :sup:`63` - 1)
    inclusive.

    :param years: will be added times 12 to ``months``
    :param months: will be truncated to :class:`int` (``int(months)``)
    :param weeks: will be added times 7 to ``days``
    :param days: will be truncated to :class:`int` (``int(days)``)
    :param hours: will be added times 3,600,000,000,000 to ``nanoseconds``
    :param minutes: will be added times 60,000,000,000 to ``nanoseconds``
    :param seconds: will be added times 1,000,000,000 to ``nanoseconds``
    :param milliseconds: will be added times 1,000,000 to ``nanoseconds``
    :param microseconds: will be added times 1,000 to ``nanoseconds``
    :param nanoseconds:
        will be truncated to :class:`int` (``int(nanoseconds)``)

    :raises ValueError: the components exceed the limits as described above.
    """

    # i64: i64:i64: i32

    min: _t.Final[Duration] = None  # type: ignore
    """The lowest duration value possible."""

    max: _t.Final[Duration] = None  # type: ignore
    """The highest duration value possible."""

    def __new__(
        cls,
        years: float = 0,
        months: float = 0,
        weeks: float = 0,
        days: float = 0,
        hours: float = 0,
        minutes: float = 0,
        seconds: float = 0,
        milliseconds: float = 0,
        microseconds: float = 0,
        nanoseconds: float = 0,
    ) -> Duration:
        mo = int(12 * years + months)
        if mo < MIN_INT64 or mo > MAX_INT64:
            raise ValueError("Months value out of range")
        d = int(7 * weeks + days)
        ns = (
            int(3600000000000 * hours)
            + int(60000000000 * minutes)
            + int(1000000000 * seconds)
            + int(1000000 * milliseconds)
            + int(1000 * microseconds)
            + int(nanoseconds)
        )
        s, ns = _symmetric_divmod(ns, _NANO_SECONDS)
        tuple_ = (mo, d, s, ns)
        avg_total_seconds = (
            mo * _AVERAGE_SECONDS_IN_MONTH
            + d * _AVERAGE_SECONDS_IN_DAY
            + s
            - (1 if ns < 0 else 0)
        )
        if not MIN_INT64 <= avg_total_seconds <= MAX_INT64:
            raise ValueError(f"Duration value out of range: {tuple_!r}")
        return tuple.__new__(cls, tuple_)

    def __bool__(self) -> bool:
        """Falsy if all primary instance attributes are."""
        return any(map(bool, self))

    __nonzero__ = __bool__

    def __add__(  # type: ignore[override]
        self, other: Duration | _timedelta
    ) -> Duration:
        """Add a :class:`.Duration` or :class:`datetime.timedelta`."""
        if isinstance(other, Duration):
            return Duration(
                months=self[0] + int(other.months),
                days=self[1] + int(other.days),
                seconds=self[2] + int(other.seconds),
                nanoseconds=self[3] + int(other.nanoseconds),
            )
        if isinstance(other, _timedelta):
            return Duration(
                months=self[0],
                days=self[1] + other.days,
                seconds=self[2] + other.seconds,
                nanoseconds=self[3] + other.microseconds * 1000,
            )
        return NotImplemented

    def __sub__(self, other: Duration | _timedelta) -> Duration:
        """Subtract a :class:`.Duration` or :class:`datetime.timedelta`."""
        if isinstance(other, Duration):
            return Duration(
                months=self[0] - int(other.months),
                days=self[1] - int(other.days),
                seconds=self[2] - int(other.seconds),
                nanoseconds=self[3] - int(other.nanoseconds),
            )
        if isinstance(other, _timedelta):
            return Duration(
                months=self[0],
                days=self[1] - other.days,
                seconds=self[2] - other.seconds,
                nanoseconds=self[3] - other.microseconds * 1000,
            )
        return NotImplemented

    def __mul__(self, other: float) -> Duration:  # type: ignore[override]
        """
        Multiply by an :class:`int` or :class:`float`.

        The operation is performed element-wise on
        ``(months, days, nanaoseconds)`` where

        * years go into months,
        * weeks go into days,
        * seconds and all sub-second units go into nanoseconds.

        Each element will be rounded to the nearest integer (.5 towards even).
        """
        if isinstance(other, (int, float)):
            return Duration(
                months=_round_half_to_even(self[0] * other),
                days=_round_half_to_even(self[1] * other),
                nanoseconds=_round_half_to_even(
                    self[2] * _NANO_SECONDS * other + self[3] * other
                ),
            )
        return NotImplemented

    def __floordiv__(self, other: int) -> Duration:  # type: ignore[override]
        """
        Integer division by an :class:`int`.

        The operation is performed element-wise on
        ``(months, days, nanaoseconds)`` where

        * years go into months,
        * weeks go into days,
        * seconds and all sub-second units go into nanoseconds.

        Each element will be rounded towards -inf.
        """
        if isinstance(other, int):
            return Duration(
                months=self[0] // other,
                days=self[1] // other,
                nanoseconds=(self[2] * _NANO_SECONDS + self[3]) // other,
            )
        return NotImplemented

    def __mod__(self, other: int) -> Duration:  # type: ignore[override]
        """
        Modulo operation by an :class:`int`.

        The operation is performed element-wise on
        ``(months, days, nanaoseconds)`` where

        * years go into months,
        * weeks go into days,
        * seconds and all sub-second units go into nanoseconds.
        """
        if isinstance(other, int):
            return Duration(
                months=self[0] % other,
                days=self[1] % other,
                nanoseconds=(self[2] * _NANO_SECONDS + self[3]) % other,
            )
        return NotImplemented

    def __divmod__(  # type: ignore[override]
        self, other: int
    ) -> tuple[Duration, Duration]:
        """
        Division and modulo operation by an :class:`int`.

        See :meth:`__floordiv__` and :meth:`__mod__`.
        """
        if isinstance(other, int):
            return self.__floordiv__(other), self.__mod__(other)
        return NotImplemented

    def __truediv__(self, other: float) -> Duration:  # type: ignore[override]
        """
        Division by an :class:`int` or :class:`float`.

        The operation is performed element-wise on
        ``(months, days, nanaoseconds)`` where

        * years go into months,
        * weeks go into days,
        * seconds and all sub-second units go into nanoseconds.

        Each element will be rounded to the nearest integer (.5 towards even).
        """
        if isinstance(other, (int, float)):
            return Duration(
                months=_round_half_to_even(self[0] / other),
                days=_round_half_to_even(self[1] / other),
                nanoseconds=_round_half_to_even(
                    self[2] * _NANO_SECONDS / other + self[3] / other
                ),
            )
        return NotImplemented

    def __pos__(self) -> Duration:
        return self

    def __neg__(self) -> Duration:
        return Duration(
            months=-self[0],
            days=-self[1],
            seconds=-self[2],
            nanoseconds=-self[3],
        )

    def __abs__(self) -> Duration:
        return Duration(
            months=abs(self[0]),
            days=abs(self[1]),
            seconds=abs(self[2]),
            nanoseconds=abs(self[3]),
        )

    def __repr__(self) -> str:
        mo, day, sec, ns = self
        return (
            "neo4j.time.Duration("
            f"months={mo!r}, "
            f"days={day!r}, "
            f"seconds={sec!r}, "
            f"nanoseconds={ns!r}"
            ")"
        )

    def __str__(self) -> str:
        return self.iso_format()

    def __reduce__(self):
        return type(self)._restore, (tuple(self), self.__dict__)

    @classmethod
    def _restore(cls, elements, dict_):
        instance = tuple.__new__(cls, elements)
        if dict_:
            instance.__dict__.update(dict_)
        return instance

    @classmethod
    def from_iso_format(cls, s: str) -> Duration:
        """
        Parse a ISO formatted duration string.

        Accepted formats (all lowercase letters are placeholders):
            'P', a zero length duration
            'PyY', y being a number of years
            'PmM', m being a number of months
            'PdD', d being a number of days

            Any combination of the above, e.g., 'P25Y1D' for 25 years and 1
            day.

            'PThH', h being a number of hours
            'PTmM', h being a number of minutes
            'PTsS', h being a number of seconds
            'PTs.sss...S', h being a fractional number of seconds

            Any combination of the above, e.g. 'PT5H1.2S' for 5 hours and 1.2
            seconds.
            Any combination of all options, e.g. 'P13MT100M' for 13 months and
            100 minutes.

        :param s: String to parse

        :raises ValueError: if the string does not match the required format.
        """
        match = _DURATION_ISO_PATTERN.match(s)
        if match:
            ns = 0
            if match.group(15):
                ns = int(match.group(15)[1:10].ljust(9, "0"))
            return cls(
                years=int(match.group(2) or 0),
                months=int(match.group(4) or 0),
                days=int(match.group(6) or 0),
                hours=int(match.group(9) or 0),
                minutes=int(match.group(11) or 0),
                seconds=int(match.group(14) or 0),
                nanoseconds=ns,
            )
        raise ValueError("Duration string must be in ISO format")

    fromisoformat = from_iso_format

    def iso_format(self, sep: str = "T") -> str:
        """
        Return the :class:`Duration` as ISO formatted string.

        :param sep: the separator before the time components.
        """
        parts = []
        hours, minutes, seconds, nanoseconds = (
            self.hours_minutes_seconds_nanoseconds
        )
        if hours:
            parts.append(f"{hours}H")
        if minutes:
            parts.append(f"{minutes}M")
        if nanoseconds:
            if seconds >= 0 and nanoseconds >= 0:
                ns_str = str(nanoseconds).rjust(9, "0").rstrip("0")
                parts.append(f"{seconds}.{ns_str}S")
            elif seconds <= 0 and nanoseconds <= 0:
                ns_str = str(abs(nanoseconds)).rjust(9, "0").rstrip("0")
                parts.append(f"-{abs(seconds)}.{ns_str}S")

            else:
                raise AssertionError("Please report this issue")
        elif seconds:
            parts.append(f"{seconds}S")
        if parts:
            parts.insert(0, sep)
        years, months, days = self.years_months_days
        if days:
            parts.insert(0, f"{days}D")
        if months:
            parts.insert(0, f"{months}M")
        if years:
            parts.insert(0, f"{years}Y")
        if parts:
            parts.insert(0, "P")
            return "".join(parts)
        else:
            return "PT0S"

    @property
    def months(self) -> int:
        """The months of the :class:`Duration`."""
        return self[0]

    @property
    def days(self) -> int:
        """The days of the :class:`Duration`."""
        return self[1]

    @property
    def seconds(self) -> int:
        """The seconds of the :class:`Duration`."""
        return self[2]

    @property
    def nanoseconds(self) -> int:
        """The nanoseconds of the :class:`Duration`."""
        return self[3]

    @property
    def years_months_days(self) -> tuple[int, int, int]:
        """
        Months and days components as a 3-tuple.

        tuple of years, months and days.
        """
        years, months = _symmetric_divmod(self[0], 12)
        return years, months, self[1]

    @property
    def hours_minutes_seconds_nanoseconds(self) -> tuple[int, int, int, int]:
        """
        Seconds and nanoseconds components as a 4-tuple.

        tuple of hours, minutes, seconds and nanoseconds.
        """
        minutes, seconds = _symmetric_divmod(self[2], 60)
        hours, minutes = _symmetric_divmod(minutes, 60)
        return hours, minutes, seconds, self[3]


Duration.min = Duration(  # type: ignore
    seconds=MIN_INT64, nanoseconds=0
)

Duration.max = Duration(  # type: ignore
    seconds=MAX_INT64, nanoseconds=999999999
)


if _t.TYPE_CHECKING:
    # make typechecker believe that Date subclasses datetime.date
    # https://github.com/python/typeshed/issues/8409#issuecomment-1197704527
    _date_base_class = _date
else:
    _date_base_class = object


class Date(_date_base_class, metaclass=_DateType):
    """
    Idealized date representation.

    A :class:`.Date` object represents a date (year, month, and day) in the
    `proleptic Gregorian Calendar
    <https://en.wikipedia.org/wiki/Proleptic_Gregorian_calendar>`_.

    Years between ``0001`` and ``9999`` are supported, with additional
    support for the "zero date" used in some contexts.

    Each date is based on a proleptic Gregorian ordinal, which models 1 Jan
    0001 as ``day 1`` and counts each subsequent day up to, and including,
    31 Dec 9999. The standard ``year``, ``month`` and ``day`` value of each
    date is also available.

    Internally, the day of the month is always stored as-is, except for the
    last three days of that month. These are always stored as -1, -2 and -3
    (counting from the last day). This system allows some temporal
    arithmetic (particularly adding or subtracting months) to produce a more
    desirable outcome than would otherwise be produced. Externally, the day
    number is always the same as would be written on a calendar.

    :param year: the year. Minimum :data:`.MIN_YEAR` (0001), maximum
        :data:`.MAX_YEAR` (9999).
    :type year: int
    :param month: the month. Minimum 1, maximum 12.
    :type month: int
    :param day: the day. Minimum 1, maximum
        :attr:`Date.days_in_month(year, month) <Date.days_in_month>`.
    :type day: int

    A zero date can also be acquired by passing all zeroes to the
    :class:`.Date` constructor or by using the :data:`ZeroDate`
    constant.
    """

    # CONSTRUCTOR #

    def __new__(cls, year: int, month: int, day: int) -> Date:
        if year == month == day == 0:
            return ZeroDate
        year, month, day = _normalize_day(year, month, day)
        ordinal = cls.__calc_ordinal(year, month, day)
        return cls.__new(ordinal, year, month, day)

    @classmethod
    def __new(cls, ordinal: int, year: int, month: int, day: int) -> Date:
        instance = object.__new__(cls)
        instance.__ordinal = int(ordinal)
        instance.__year = int(year)
        instance.__month = int(month)
        instance.__day = int(day)
        return instance

    # CLASS METHODS #

    @classmethod
    def today(cls, tz: _tzinfo | None = None) -> Date:
        """
        Get the current date.

        :param tz: timezone or None to get the local :class:`.Date`.

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls._from_clock_time(_Clock().local_time(), UnixEpoch)
        else:
            return (
                DateTime.utc_now()
                .replace(tzinfo=_timezone.utc)
                .astimezone(tz)
                .date()
            )

    @classmethod
    def utc_today(cls) -> Date:
        """Get the current date as UTC local date."""
        return cls._from_clock_time(_Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_timestamp(
        cls, timestamp: float, tz: _tzinfo | None = None
    ) -> Date:
        """
        Construct :class:`.Date` from a time stamp (seconds since unix epoch).

        :param timestamp: the unix timestamp (seconds since unix epoch).
        :param tz: timezone. Set to None to create a local :class:`.Date`.

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        return cls.from_native(_datetime.fromtimestamp(timestamp, tz))

    @classmethod
    def utc_from_timestamp(cls, timestamp: float) -> Date:
        """
        Construct :class:`.Date` from a time stamp (seconds since unix epoch).

        :returns: the :class:`Date` as local date :class:`Date` in UTC.
        """
        return cls._from_clock_time((timestamp, 0), UnixEpoch)

    @classmethod
    def from_ordinal(cls, ordinal: int) -> Date:
        """
        Construct :class:`.Date` from the proleptic Gregorian ordinal.

        ``0001-01-01`` has ordinal 1 and ``9999-12-31`` has ordinal 3,652,059.
        Values outside of this range trigger a :exc:`ValueError`.
        The corresponding instance method for the reverse date-to-ordinal
        transformation is :meth:`.to_ordinal`.
        The ordinal 0 has a special semantic and will return :attr:`ZeroDate`.

        :raises ValueError: if the ordinal is outside the range [0, 3652059]
            (both values included).
        """
        if ordinal == 0:
            return ZeroDate
        elif ordinal < 0 or ordinal > 3652059:
            raise ValueError("Ordinal out of range (0..3652059)")
        d = _datetime.fromordinal(ordinal)
        year, month, day = _normalize_day(d.year, d.month, d.day)
        return cls.__new(ordinal, year, month, day)

    @classmethod
    def parse(cls, s: str) -> Date:
        """
        Parse a string to produce a :class:`.Date`.

        Accepted formats:
            'Y-M-D'

        :param s: the string to be parsed.

        :raises ValueError: if the string could not be parsed.
        """
        try:
            numbers = list(map(int, s.split("-")))
        except (ValueError, AttributeError):
            raise ValueError(
                "Date string must be in format YYYY-MM-DD"
            ) from None
        else:
            if len(numbers) == 3:
                return cls(*numbers)
            raise ValueError("Date string must be in format YYYY-MM-DD")

    @classmethod
    def from_iso_format(cls, s: str) -> Date:
        """
        Parse a ISO formatted Date string.

        Accepted formats:
            'YYYY-MM-DD'

        :param s: the string to be parsed.

        :raises ValueError: if the string could not be parsed.
        """
        m = _DATE_ISO_PATTERN.match(s)
        if m:
            year = int(m.group(1))
            month = int(m.group(2))
            day = int(m.group(3))
            return cls(year, month, day)
        raise ValueError("Date string must be in format YYYY-MM-DD")

    @classmethod
    def from_native(cls, d: _date) -> Date:
        """
        Convert from a native Python :class:`datetime.date` value.

        :param d: the date to convert.
        """
        return Date.from_ordinal(d.toordinal())

    # TODO: 7.0 - remove public alias (copy over docstring)
    @classmethod
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> Date:
        """
        Convert from a ClockTime relative to a given epoch.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :param epoch: the epoch to which ``clock_time`` is relative

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return cls._from_clock_time(clock_time, epoch)

    @classmethod
    def _from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> Date:
        try:
            clock_time = _ClockTime(*clock_time)
        except (TypeError, ValueError):
            raise ValueError(
                "Clock time must be a 2-tuple of (s, ns)"
            ) from None
        else:
            ordinal = clock_time.seconds // 86400
            return Date.from_ordinal(ordinal + epoch.date().to_ordinal())

    @classmethod
    def is_leap_year(cls, year: int) -> bool:
        """
        Indicate whether ``year`` is a leap year.

        :param year: the year to look up

        :raises ValueError: if ``year`` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError(f"Year out of range ({MIN_YEAR}..{MAX_YEAR})")
        return _IS_LEAP_YEAR[year]

    @classmethod
    def days_in_year(cls, year: int) -> int:
        """
        Return the number of days in ``year``.

        :param year: the year to look up

        :raises ValueError: if ``year`` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError(f"Year out of range ({MIN_YEAR}..{MAX_YEAR})")
        return _DAYS_IN_YEAR[year]

    @classmethod
    def days_in_month(cls, year: int, month: int) -> int:
        """
        Return the number of days in ``month`` of ``year``.

        :param year: the year to look up
        :param month: the month to look up

        :raises ValueError: if ``year`` or ``month`` is out of range:
            :attr:`MIN_YEAR` <= year <= :attr:`MAX_YEAR`;
            1 <= year <= 12
        """
        if year < MIN_YEAR or year > MAX_YEAR:
            raise ValueError(f"Year out of range ({MIN_YEAR}..{MAX_YEAR})")
        if month < 1 or month > 12:
            raise ValueError("Month out of range (1..12)")
        return _DAYS_IN_MONTH[year, month]

    @classmethod
    def __calc_ordinal(cls, year, month, day):
        if day < 0:
            day = cls.days_in_month(year, month) + int(day) + 1
        # The built-in date class does this faster than a
        # long-hand pure Python algorithm could
        return _date(year, month, day).toordinal()

    # CLASS METHOD ALIASES #

    if _t.TYPE_CHECKING:

        @classmethod
        def fromisoformat(cls, s: str) -> Date: ...

        @classmethod
        def fromordinal(cls, ordinal: int) -> Date: ...

        @classmethod
        def fromtimestamp(
            cls, timestamp: float, tz: _tzinfo | None = None
        ) -> Date: ...

        @classmethod
        def utcfromtimestamp(cls, timestamp: float) -> Date: ...

    # CLASS ATTRIBUTES #

    min: _t.Final[Date] = None  # type: ignore
    """The earliest date value possible."""

    max: _t.Final[Date] = None  # type: ignore
    """The latest date value possible."""

    resolution: _t.Final[Duration] = None  # type: ignore
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    __ordinal = 0

    __year = 0

    __month = 0

    __day = 0

    @property
    def year(self) -> int:
        """
        The year of the date.

        :type: int
        """
        return self.__year

    @property
    def month(self) -> int:
        """
        The month of the date.

        :type: int
        """
        return self.__month

    @property
    def day(self) -> int:
        """
        The day of the date.

        :type: int
        """
        if self.__day == 0:
            return 0
        if self.__day >= 1:
            return self.__day
        return self.days_in_month(self.__year, self.__month) + self.__day + 1

    @property
    def year_month_day(self) -> tuple[int, int, int]:
        """3-tuple of (year, month, day) describing the date."""
        return self.year, self.month, self.day

    @property
    def year_week_day(self) -> tuple[int, int, int]:
        """
        3-tuple of (year, week_of_year, day_of_week) describing the date.

        ``day_of_week`` will be 1 for Monday and 7 for Sunday.
        """
        ordinal = self.__ordinal
        year = self.__year

        def day_of_week(o):
            return ((o - 1) % 7) + 1

        def iso_week_1(y):
            j4 = Date(y, 1, 4)
            return j4 + Duration(days=(1 - day_of_week(j4.to_ordinal())))

        if ordinal >= Date(year, 12, 29).to_ordinal():
            week1 = iso_week_1(year + 1)
            if ordinal < week1.to_ordinal():
                week1 = iso_week_1(year)
            else:
                year += 1
        else:
            week1 = iso_week_1(year)
            if ordinal < week1.to_ordinal():
                year -= 1
                week1 = iso_week_1(year)
        return (
            year,
            int((ordinal - week1.to_ordinal()) / 7 + 1),
            day_of_week(ordinal),
        )

    @property
    def year_day(self) -> tuple[int, int]:
        """
        2-tuple of (year, day_of_the_year) describing the date.

        This is the number of the day relative to the start of the year,
        with ``1 Jan`` corresponding to ``1``.
        """
        return (
            self.__year,
            self.toordinal() - Date(self.__year, 1, 1).toordinal() + 1,
        )

    # OPERATIONS #

    def __hash__(self):
        return hash(self.toordinal())

    def __eq__(self, other: object) -> bool:
        """``==`` comparison with :class:`.Date` or :class:`datetime.date`."""
        if not isinstance(other, (Date, _date)):
            return NotImplemented
        return self.toordinal() == other.toordinal()

    def __lt__(self, other: Date | _date) -> bool:
        """``<`` comparison with :class:`.Date` or :class:`datetime.date`."""
        if not isinstance(other, (Date, _date)):
            return NotImplemented
        return self.toordinal() < other.toordinal()

    def __le__(self, other: Date | _date) -> bool:
        """``<=`` comparison with :class:`.Date` or :class:`datetime.date`."""
        if not isinstance(other, (Date, _date)):
            return NotImplemented
        return self.toordinal() <= other.toordinal()

    def __ge__(self, other: Date | _date) -> bool:
        """``>=`` comparison with :class:`.Date` or :class:`datetime.date`."""
        if not isinstance(other, (Date, _date)):
            return NotImplemented
        return self.toordinal() >= other.toordinal()

    def __gt__(self, other: Date | _date) -> bool:
        """``>`` comparison with :class:`.Date` or :class:`datetime.date`."""
        if not isinstance(other, (Date, _date)):
            return NotImplemented
        return self.toordinal() > other.toordinal()

    def __add__(self, other: Duration) -> Date:  # type: ignore[override]
        """
        Add a :class:`.Duration`.

        :raises ValueError: if the added duration has a time component.
        """

        def add_months(d, months):
            overflow_years, month = divmod(months + d.__month - 1, 12)
            d.__year += overflow_years
            d.__month = month + 1

        def add_days(d, days):
            assert 1 <= d.__day <= 28 or -28 <= d.__day <= -1
            if d.__day >= 1:
                new_days = d.__day + days
                if 1 <= new_days <= 27:
                    d.__day = new_days
                    return
            d0 = Date.from_ordinal(d.__ordinal + days)
            d.__year, d.__month, d.__day = d0.__year, d0.__month, d0.__day

        if isinstance(other, Duration):
            if other.seconds or other.nanoseconds:
                raise ValueError(
                    "Cannot add a Duration with seconds or nanoseconds to a "
                    "Date"
                )
            if other.months == other.days == 0:
                return self
            new_date = self.replace()
            # Add days before months as the former sometimes
            # requires the current ordinal to be correct.
            if other.days:
                add_days(new_date, other.days)
            if other.months:
                add_months(new_date, other.months)
            new_date.__ordinal = self.__calc_ordinal(
                new_date.year, new_date.month, new_date.day
            )
            return new_date
        return NotImplemented

    @_t.overload  # type: ignore[override]
    def __sub__(self, other: Date | _date) -> Duration: ...

    @_t.overload
    def __sub__(self, other: Duration) -> Date: ...

    def __sub__(self, other):
        """
        Subtract a :class:`.Date` or :class:`.Duration`.

        :returns: If a :class:`.Date` is subtracted, the time between the two
            dates is returned as :class:`.Duration`. If a :class:`.Duration` is
            subtracted, a new :class:`.Date` is returned.
        :rtype: Date or Duration

        :raises ValueError: if the added duration has a time component.
        """
        if isinstance(other, (Date, _date)):
            return Duration(days=(self.toordinal() - other.toordinal()))
        try:
            return self.__add__(-other)
        except TypeError:
            return NotImplemented

    def __reduce__(self):
        if self is ZeroDate:
            return "ZeroDate"
        return type(self)._restore, (self.__dict__,)

    @classmethod
    def _restore(cls, dict_) -> Date:
        instance = object.__new__(cls)
        if dict_:
            instance.__dict__.update(dict_)
        return instance

    # INSTANCE METHODS #

    if _t.TYPE_CHECKING:

        def replace(
            self,
            year: _t.SupportsIndex = ...,
            month: _t.SupportsIndex = ...,
            day: _t.SupportsIndex = ...,
            **kwargs: object,
        ) -> Date: ...

    else:

        def replace(self, **kwargs) -> Date:
            """
            Return a :class:`.Date` with one or more components replaced.

            :Keyword Arguments:
               * **year** (:class:`typing.SupportsIndex`):
                 overwrite the year - default: ``self.year``
               * **month** (:class:`typing.SupportsIndex`):
                 overwrite the month - default: ``self.month``
               * **day** (:class:`typing.SupportsIndex`):
                 overwrite the day - default: ``self.day``
            """
            return Date(
                int(kwargs.get("year", self.__year)),
                int(kwargs.get("month", self.__month)),
                int(kwargs.get("day", self.__day)),
            )

    def time_tuple(self) -> _struct_time:
        """Convert the date to :class:`time.struct_time`."""
        _, _, day_of_week = self.year_week_day
        _, day_of_year = self.year_day
        return _struct_time(
            (
                self.year,
                self.month,
                self.day,
                0,
                0,
                0,
                day_of_week - 1,
                day_of_year,
                -1,
            )
        )

    def to_ordinal(self) -> int:
        """
        Get the date's proleptic Gregorian ordinal.

        The corresponding class method for the reverse ordinal-to-date
        transformation is :meth:`.Date.from_ordinal`.
        """
        return self.__ordinal

    # TODO: 7.0 - remove public alias (copy over docstring)
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def to_clock_time(self, epoch: Date | DateTime) -> ClockTime:
        """
        Convert the date to :class:`ClockTime` relative to ``epoch``'s date.

        :param epoch: the epoch to which the date is relative

        :returns: the :class:`ClockTime` value.

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return self._to_clock_time(epoch)

    def _to_clock_time(self, epoch: Date | DateTime) -> ClockTime:
        try:
            return _ClockTime(86400 * (self.to_ordinal() - epoch.to_ordinal()))
        except AttributeError:
            raise TypeError("Epoch has no ordinal value") from None

    def to_native(self) -> _date:
        """Convert to a native Python :class:`datetime.date` value."""
        return _date.fromordinal(self.to_ordinal())

    def weekday(self) -> int:
        """Get the day of the week where Monday is 0 and Sunday is 6."""
        return self.year_week_day[2] - 1

    def iso_weekday(self) -> int:
        """Get the day of the week where Monday is 1 and Sunday is 7."""
        return self.year_week_day[2]

    def iso_calendar(self) -> tuple[int, int, int]:
        """Return :attr:`.year_week_day`."""
        return self.year_week_day

    def iso_format(self) -> str:
        """Return the :class:`.Date` as ISO formatted string."""
        if self.__ordinal == 0:
            return "0000-00-00"
        year, month, day = self.year_month_day
        return f"{year:04d}-{month:02d}-{day:02d}"

    def __repr__(self) -> str:
        if self.__ordinal == 0:
            return "neo4j.time.ZeroDate"
        year, month, day = self.year_month_day
        return f"neo4j.time.Date({year!r}, {month!r}, {day!r})"

    def __str__(self) -> str:
        return self.iso_format()

    def __format__(self, format_spec):
        if not format_spec:
            return self.iso_format()
        format_spec = _FORMAT_F_REPLACE.sub("000000000", format_spec)
        return self.to_native().__format__(format_spec)

    # INSTANCE METHOD ALIASES #

    def __getattr__(self, name):
        """
        Get attribute by name.

        Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "isocalendar": self.iso_calendar,
                "isoformat": self.iso_format,
                "isoweekday": self.iso_weekday,
                "strftime": self.__format__,
                "toordinal": self.to_ordinal,
                "timetuple": self.time_tuple,
            }[name]
        except KeyError:
            raise AttributeError(f"Date has no attribute {name!r}") from None

    if _t.TYPE_CHECKING:

        def iso_calendar(self) -> tuple[int, int, int]: ...

        isoformat = iso_format
        isoweekday = iso_weekday
        strftime = __format__
        toordinal = to_ordinal
        timetuple = time_tuple


Date.min = Date.from_ordinal(1)  # type: ignore
Date.max = Date.from_ordinal(3652059)  # type: ignore
Date.resolution = Duration(days=1)  # type: ignore

#: A :class:`neo4j.time.Date` instance set to ``0000-00-00``.
#: This has an ordinal value of ``0``.
ZeroDate = object.__new__(Date)


if _t.TYPE_CHECKING:
    # make typechecker believe that Time subclasses datetime.time
    # https://github.com/python/typeshed/issues/8409#issuecomment-1197704527
    _time_base_class = _time
else:
    _time_base_class = object


def _dst(
    tz: _tzinfo | None = None, dt: DateTime | None = None
) -> _timedelta | None:
    if tz is None:
        return None
    try:
        value = tz.dst(dt)
    except TypeError:
        if dt is None:
            raise
        # For timezone implementations not compatible with the custom
        # datetime implementations, we can't do better than this.
        value = tz.dst(dt.to_native())  # type: ignore
    if value is None:
        return None
    if isinstance(value, _timedelta):
        if value.days != 0:
            raise ValueError("dst must be less than a day")
        if value.seconds % 60 != 0 or value.microseconds != 0:
            raise ValueError("dst must be a whole number of minutes")
        return value
    raise TypeError("dst must be a timedelta")


def _tz_name(tz: _tzinfo | None, dt: DateTime | None) -> str | None:
    if tz is None:
        return None
    try:
        return tz.tzname(dt)
    except TypeError:
        if dt is None:
            raise
        # For timezone implementations not compatible with the custom
        # datetime implementations, we can't do better than this.
        return tz.tzname(dt.to_native())


class Time(_time_base_class, metaclass=_TimeType):
    """
    Time of day.

    The :class:`.Time` class is a nanosecond-precision drop-in replacement for
    the standard library :class:`datetime.time` class.

    A high degree of API compatibility with the standard library classes is
    provided.

    :class:`neo4j.time.Time` objects introduce the concept of ``ticks``.
    This is simply a count of the number of nanoseconds since midnight,
    in many ways analogous to the :class:`neo4j.time.Date` ordinal.
    ``ticks`` values are integers, with a minimum value of ``0`` and a maximum
    of ``86_399_999_999_999``.

    Local times are represented by :class:`.Time` with no ``tzinfo``.

    :param hour: the hour of the time. Must be in range 0 <= hour < 24.
    :param minute: the minute of the time. Must be in range 0 <= minute < 60.
    :param second: the second of the time. Must be in range 0 <= second < 60.
    :param nanosecond: the nanosecond of the time.
        Must be in range 0 <= nanosecond < 999999999.
    :param tzinfo: timezone or None to get a local :class:`.Time`.

    :raises ValueError: if one of the parameters is out of range.

    .. versionchanged:: 5.0
        The parameter ``second`` no longer accepts :class:`float` values.
    """

    # CONSTRUCTOR #

    def __init__(
        self,
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
        nanosecond: int = 0,
        tzinfo: _tzinfo | None = None,
    ) -> None:
        hour, minute, second, nanosecond = self.__normalize_nanosecond(
            hour, minute, second, nanosecond
        )
        ticks = (
            3600000000000 * hour
            + 60000000000 * minute
            + 1000000000 * second
            + nanosecond
        )
        self.__unchecked_init(ticks, hour, minute, second, nanosecond, tzinfo)

    @classmethod
    def __unchecked_new(
        cls,
        ticks: int,
        hour: int,
        minutes: int,
        second: int,
        nano: int,
        tz: _tzinfo | None,
    ) -> Time:
        instance = object.__new__(Time)
        instance.__unchecked_init(ticks, hour, minutes, second, nano, tz)
        return instance

    def __unchecked_init(
        self,
        ticks: int,
        hour: int,
        minutes: int,
        second: int,
        nano: int,
        tz: _tzinfo | None,
    ) -> None:
        self.__ticks = ticks
        self.__hour = hour
        self.__minute = minutes
        self.__second = second
        self.__nanosecond = nano
        self.__tzinfo = tz

    # CLASS METHODS #

    @classmethod
    def now(cls, tz: _tzinfo | None = None) -> Time:
        """
        Get the current time.

        :param tz: optional timezone

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls._from_clock_time(_Clock().local_time(), UnixEpoch)
        else:
            return (
                DateTime.utc_now()
                .replace(tzinfo=_timezone.utc)
                .astimezone(tz)
                .timetz()
            )

    @classmethod
    def utc_now(cls) -> Time:
        """Get the current time as UTC local time."""
        return cls._from_clock_time(_Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_iso_format(cls, s: str) -> Time:
        """
        Parse a ISO formatted time string.

        Accepted formats:
            Local times:
                'hh'
                'hh:mm'
                'hh:mm:ss'
                'hh:mm:ss.ssss...'
            Times with timezones (UTC offset):
                '<local time>+hh:mm'
                '<local time>+hh:mm:ss'
                '<local time>+hh:mm:ss.ssss....'
                '<local time>-hh:mm'
                '<local time>-hh:mm:ss'
                '<local time>-hh:mm:ss.ssss....'

                Where the UTC offset will only respect hours and minutes.
                Seconds and sub-seconds are ignored.

        :param s: String to parse

        :raises ValueError: if the string does not match the required format.
        """
        from pytz import FixedOffset  # type: ignore

        m = _TIME_ISO_PATTERN.match(s)
        if m:
            hour = int(m.group(1))
            minute = int(m.group(3) or 0)
            second = int(m.group(6) or 0)
            nanosecond = m.group(7)
            if nanosecond:
                nanosecond = int(nanosecond[1:10].ljust(9, "0"))
            else:
                nanosecond = 0
            if m.group(8) is None:
                return cls(hour, minute, second, nanosecond)
            else:
                offset_multiplier = 1 if m.group(9) == "+" else -1
                offset_hour = int(m.group(10))
                offset_minute = int(m.group(11))
                # pytz only supports offsets of minute resolution
                # so we can ignore this part
                # offset_second = float(m.group(13) or 0.0)
                offset = 60 * offset_hour + offset_minute
                tz = FixedOffset(offset_multiplier * offset)
                return cls(hour, minute, second, nanosecond, tzinfo=tz)
        raise ValueError("Time string is not in ISO format")

    @classmethod
    def from_ticks(cls, ticks: int, tz: _tzinfo | None = None) -> Time:
        """
        Create a time from ticks (nanoseconds since midnight).

        :param ticks: nanoseconds since midnight
        :param tz: optional timezone

        :raises ValueError: if ticks is out of bounds
            (0 <= ticks < 86400000000000)

        .. versionchanged:: 5.0
            The parameter ``ticks`` no longer accepts :class:`float` values
            but only :class:`int`. It's now nanoseconds since midnight instead
            of seconds.
        """
        if not isinstance(ticks, int):
            raise TypeError("Ticks must be int")
        if 0 <= ticks < 86400000000000:
            second, nanosecond = divmod(ticks, _NANO_SECONDS)
            minute, second = divmod(second, 60)
            hour, minute = divmod(minute, 60)
            return cls.__unchecked_new(
                ticks, hour, minute, second, nanosecond, tz
            )
        raise ValueError("Ticks out of range (0..86400000000000)")

    @classmethod
    def from_native(cls, t: _time) -> Time:
        """
        Convert from a native Python :class:`datetime.time` value.

        :param t: time to convert from
        """
        nanosecond = t.microsecond * 1000
        return Time(t.hour, t.minute, t.second, nanosecond, t.tzinfo)

    # TODO: 7.0 - remove public alias (copy over docstring)
    @classmethod
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> Time:
        """
        Convert from a :class:`.ClockTime` relative to a given epoch.

        This method, in contrast to most others of this package, assumes days
        of exactly 24 hours.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :param epoch: the epoch to which ``clock_time`` is relative

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return cls._from_clock_time(clock_time, epoch)

    @classmethod
    def _from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> Time:
        clock_time = _ClockTime(*clock_time)
        ts = clock_time.seconds % 86400
        nanoseconds = int(_NANO_SECONDS * ts + clock_time.nanoseconds)
        ticks = (epoch.time().ticks + nanoseconds) % (86400 * _NANO_SECONDS)
        return Time.from_ticks(ticks)

    @classmethod
    def __normalize_hour(cls, hour):
        hour = int(hour)
        if 0 <= hour < 24:
            return hour
        raise ValueError("Hour out of range (0..23)")

    @classmethod
    def __normalize_minute(cls, hour, minute):
        hour = cls.__normalize_hour(hour)
        minute = int(minute)
        if 0 <= minute < 60:
            return hour, minute
        raise ValueError("Minute out of range (0..59)")

    @classmethod
    def __normalize_second(cls, hour, minute, second):
        hour, minute = cls.__normalize_minute(hour, minute)
        second = int(second)
        if 0 <= second < 60:
            return hour, minute, second
        raise ValueError("Second out of range (0..59)")

    @classmethod
    def __normalize_nanosecond(cls, hour, minute, second, nanosecond):
        hour, minute, second = cls.__normalize_second(hour, minute, second)
        if 0 <= nanosecond < _NANO_SECONDS:
            return hour, minute, second, nanosecond
        raise ValueError(f"Nanosecond out of range (0..{_NANO_SECONDS - 1})")

    # CLASS METHOD ALIASES #

    if _t.TYPE_CHECKING:

        @classmethod
        def from_iso_format(cls, s: str) -> Time: ...

        @classmethod
        def utc_now(cls) -> Time: ...

    # CLASS ATTRIBUTES #

    min: _t.Final[Time] = None  # type: ignore
    """The earliest time value possible."""

    max: _t.Final[Time] = None  # type: ignore
    """The latest time value possible."""

    resolution: _t.Final[Duration] = None  # type: ignore
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    __ticks = 0

    __hour = 0

    __minute = 0

    __second = 0

    __nanosecond = 0

    __tzinfo: _tzinfo | None = None

    @property
    def ticks(self) -> int:
        """
        The total number of nanoseconds since midnight.

        .. versionchanged:: 5.0
            The property's type changed from :class:`float` to :class:`int`.
            It's now nanoseconds since midnight instead of seconds.
        """
        return self.__ticks

    @property
    def hour(self) -> int:
        """The hours of the time."""
        return self.__hour

    @property
    def minute(self) -> int:
        """The minutes of the time."""
        return self.__minute

    @property
    def second(self) -> int:
        """
        The seconds of the time.

        .. versionchanged:: 4.4
            The property's type changed from :class:`float` to
            :class:`decimal.Decimal` to mitigate rounding issues.

        .. versionchanged:: 5.0
            The  property's type changed from :class:`decimal.Decimal` to
            :class:`int`. It does not longer cary sub-second information.
            Use :attr:`nanosecond` instead.
        """
        return self.__second

    @property
    def nanosecond(self) -> int:
        """The nanoseconds of the time."""
        return self.__nanosecond

    @property
    def hour_minute_second_nanosecond(self) -> tuple[int, int, int, int]:
        """The time as a tuple of (hour, minute, second, nanosecond)."""
        return self.__hour, self.__minute, self.__second, self.__nanosecond

    @property
    def tzinfo(self) -> _tzinfo | None:
        """The timezone of this time."""
        return self.__tzinfo

    # OPERATIONS #

    @_t.overload
    def _get_both_normalized_ticks(
        self, other: Time | _time, strict: _t.Literal[True] = True
    ) -> tuple[int, int]: ...

    @_t.overload
    def _get_both_normalized_ticks(
        self, other: Time | _time, strict: _t.Literal[False]
    ) -> tuple[int, int] | None: ...

    def _get_both_normalized_ticks(
        self, other: Time | _time, strict: bool = True
    ) -> tuple[int, int] | None:
        if (self.utc_offset() is None) ^ (other.utcoffset() is None):
            if strict:
                raise TypeError(
                    "can't compare offset-naive and offset-aware times"
                )
            else:
                return None
        other_ticks: int
        if isinstance(other, Time):
            other_ticks = other.__ticks
        else:
            assert isinstance(other, _time)
            other_ticks = int(
                3600000000000 * other.hour
                + 60000000000 * other.minute
                + _NANO_SECONDS * other.second
                + 1000 * other.microsecond
            )
        utc_offset: _timedelta | None = other.utcoffset()
        if utc_offset is not None:
            other_ticks -= int(utc_offset.total_seconds() * _NANO_SECONDS)
        self_ticks = self.__ticks
        utc_offset = self.utc_offset()
        if utc_offset is not None:
            self_ticks -= int(utc_offset.total_seconds() * _NANO_SECONDS)
        return self_ticks, other_ticks

    def __hash__(self):
        if self.__nanosecond % 1000 == 0:
            return hash(self.to_native())
        self_ticks = self.__ticks
        if self.utc_offset() is not None:
            self_ticks -= self.utc_offset().total_seconds() * _NANO_SECONDS
        return hash(self_ticks)

    def __eq__(self, other: object) -> bool:
        """`==` comparison with :class:`.Time` or :class:`datetime.time`."""
        if not isinstance(other, (Time, _time)):
            return NotImplemented
        ticks = self._get_both_normalized_ticks(other, strict=False)
        if ticks is None:
            return False
        self_ticks, other_ticks = ticks
        return self_ticks == other_ticks

    def __lt__(self, other: Time | _time) -> bool:
        """`<` comparison with :class:`.Time` or :class:`datetime.time`."""
        if not isinstance(other, (Time, _time)):
            return NotImplemented
        self_ticks, other_ticks = self._get_both_normalized_ticks(other)
        return self_ticks < other_ticks

    def __le__(self, other: Time | _time) -> bool:
        """`<=` comparison with :class:`.Time` or :class:`datetime.time`."""
        if not isinstance(other, (Time, _time)):
            return NotImplemented
        self_ticks, other_ticks = self._get_both_normalized_ticks(other)
        return self_ticks <= other_ticks

    def __ge__(self, other: Time | _time) -> bool:
        """`>=` comparison with :class:`.Time` or :class:`datetime.time`."""
        if not isinstance(other, (Time, _time)):
            return NotImplemented
        self_ticks, other_ticks = self._get_both_normalized_ticks(other)
        return self_ticks >= other_ticks

    def __gt__(self, other: Time | _time) -> bool:
        """`>` comparison with :class:`.Time` or :class:`datetime.time`."""
        if not isinstance(other, (Time, _time)):
            return NotImplemented
        self_ticks, other_ticks = self._get_both_normalized_ticks(other)
        return self_ticks > other_ticks

    # INSTANCE METHODS #

    if _t.TYPE_CHECKING:

        def replace(  # type: ignore[override]
            self,
            hour: _t.SupportsIndex = ...,
            minute: _t.SupportsIndex = ...,
            second: _t.SupportsIndex = ...,
            nanosecond: _t.SupportsIndex = ...,
            tzinfo: _tzinfo | None = ...,
            **kwargs: object,
        ) -> Time: ...

    else:

        def replace(self, **kwargs) -> Time:
            """
            Return a :class:`.Time` with one or more components replaced.

            :Keyword Arguments:
               * **hour** (:class:`typing.SupportsIndex`):
                 overwrite the hour - default: ``self.hour``
               * **minute** (:class:`typing.SupportsIndex`):
                 overwrite the minute - default: ``self.minute``
               * **second** (:class:`typing.SupportsIndex`):
                 overwrite the second - default: ``int(self.second)``
               * **nanosecond** (:class:`typing.SupportsIndex`):
                 overwrite the nanosecond - default: ``self.nanosecond``
               * **tzinfo** (:class:`datetime.tzinfo` or :data:`None`):
                 overwrite the timezone - default: ``self.tzinfo``
            """
            return Time(
                hour=int(kwargs.get("hour", self.__hour)),
                minute=int(kwargs.get("minute", self.__minute)),
                second=int(kwargs.get("second", self.__second)),
                nanosecond=int(kwargs.get("nanosecond", self.__nanosecond)),
                tzinfo=kwargs.get("tzinfo", self.__tzinfo),
            )

    def _utc_offset(self, dt=None):
        if self.tzinfo is None:
            return None
        try:
            value = self.tzinfo.utcoffset(dt)
        except TypeError:
            # For timezone implementations not compatible with the custom
            # datetime implementations, we can't do better than this.
            value = self.tzinfo.utcoffset(dt.to_native())
        if value is None:
            return None
        if isinstance(value, _timedelta):
            s = value.total_seconds()
            if not (-86400 < s < 86400):
                raise ValueError("utcoffset must be less than a day")
            if s % 60 != 0 or value.microseconds != 0:
                raise ValueError("utcoffset must be a whole number of minutes")
            return value
        raise TypeError("utcoffset must be a timedelta")

    def utc_offset(self) -> _timedelta | None:
        """
        Return the UTC offset of this time.

        :returns: None if this is a local time (:attr:`.tzinfo` is None), else
            returns ``self.tzinfo.utcoffset(self)``.

        :raises ValueError: if ``self.tzinfo.utcoffset(self)`` is not None and
            a :class:`timedelta` with a magnitude greater equal 1 day or that
            is not a whole number of minutes.
        :raises TypeError: if `self.tzinfo.utcoffset(self)` does return
            anything but :data:`None` or a :class:`datetime.timedelta`.
        """
        return self._utc_offset()

    def dst(self) -> _timedelta | None:
        """
        Get the daylight saving time adjustment (DST).

        :returns: None if this is a local time (:attr:`.tzinfo` is None), else
            returns ``self.tzinfo.dst(self)``.

        :raises ValueError: if ``self.tzinfo.dst(self)`` is not None and a
            :class:`timedelta` with a magnitude greater equal 1 day or that is
            not a whole number of minutes.
        :raises TypeError: if ``self.tzinfo.dst(self)`` does return anything
            but None or a :class:`datetime.timedelta`.
        """
        return _dst(self.tzinfo, None)

    def tzname(self) -> str | None:
        """
        Get the name of the :class:`.Time`'s timezone.

        :returns: None if the time is local (i.e., has no timezone), else
            return ``self.tzinfo.tzname(self)``
        """
        return _tz_name(self.tzinfo, None)

    # TODO: 7.0 - remove public alias (copy over docstring)
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def to_clock_time(self) -> ClockTime:
        """
        Convert to :class:`.ClockTime`.

        The returned :class:`.ClockTime` is relative to :attr:`Time.min`.

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return self._to_clock_time()

    def _to_clock_time(self) -> ClockTime:
        seconds, nanoseconds = divmod(self.ticks, _NANO_SECONDS)
        return _ClockTime(seconds, nanoseconds)

    def to_native(self) -> _time:
        """
        Convert to a native Python :class:`datetime.time` value.

        This conversion is lossy as the native time implementation only
        supports a resolution of microseconds instead of nanoseconds.
        """
        h, m, s, ns = self.hour_minute_second_nanosecond
        µs = _round_half_to_even(ns / 1000)
        tz = self.tzinfo
        return _time(h, m, s, µs, tz)

    def iso_format(self) -> str:
        """Return the :class:`.Time` as ISO formatted string."""
        h, m, s, ns = self.hour_minute_second_nanosecond
        res = f"{h:02d}:{m:02d}:{s:02d}.{ns:09d}"
        offset = self.utc_offset()
        if offset is not None:
            offset_hours, offset_minutes = divmod(
                int(offset.total_seconds() // 60), 60
            )
            res += f"{offset_hours:+03d}:{offset_minutes:02d}"
        return res

    def __repr__(self) -> str:
        fields: tuple[str, ...]
        if self.tzinfo is None:
            fields = tuple(map(repr, self.hour_minute_second_nanosecond))
        else:
            fields = (
                *map(repr, self.hour_minute_second_nanosecond),
                f"tzinfo={self.tzinfo!r}",
            )

        return f"neo4j.time.Time({', '.join(fields)})"

    def __str__(self) -> str:
        return self.iso_format()

    def __format__(self, format_spec):
        if not format_spec:
            return self.iso_format()
        format_spec = _FORMAT_F_REPLACE.sub(
            f"{self.__nanosecond:09}", format_spec
        )
        return self.to_native().__format__(format_spec)

    # INSTANCE METHOD ALIASES #

    def __getattr__(self, name):
        """
        Get attribute by name.

        Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "isoformat": self.iso_format,
                "utcoffset": self.utc_offset,
            }[name]
        except KeyError:
            raise AttributeError(f"Date has no attribute {name!r}") from None

    if _t.TYPE_CHECKING:

        def isoformat(self) -> str:  # type: ignore[override]
            ...

        utcoffset = utc_offset


Time.min = Time(  # type: ignore
    hour=0, minute=0, second=0, nanosecond=0
)
Time.max = Time(  # type: ignore
    hour=23, minute=59, second=59, nanosecond=999999999
)
Time.resolution = Duration(  # type: ignore
    nanoseconds=1
)

#: A :class:`.Time` instance set to ``00:00:00``.
#: This has a :attr:`.ticks` value of ``0``.
Midnight: _t.Final[Time] = Time.min

#: A :class:`.Time` instance set to ``12:00:00``.
#: This has a :attr:`.ticks` value of ``43200000000000``.
Midday: _t.Final[Time] = Time(hour=12)


if _t.TYPE_CHECKING:
    # make typechecker believe that DateTime subclasses datetime.datetime
    # https://github.com/python/typeshed/issues/8409#issuecomment-1197704527
    _date_time_base_class = _datetime
else:
    _date_time_base_class = object


@_total_ordering
class DateTime(_date_time_base_class, metaclass=_DateTimeType):
    """
    A point in time represented as a date and a time.

    The :class:`.DateTime` class is a nanosecond-precision drop-in replacement
    for the standard library :class:`datetime.datetime` class.

    As such, it contains both :class:`.Date` and :class:`.Time` information and
    draws functionality from those individual classes.

    A :class:`.DateTime` object is fully compatible with the Python time zone
    library `pytz <https://pypi.org/project/pytz/>`_. Functions such as
    ``normalize`` and ``localize`` can be used in the same way as they are with
    the standard library classes.

    Regular construction of a :class:`.DateTime` object requires at
    least the ``year``, ``month`` and ``day`` arguments to be supplied. The
    optional ``hour``, ``minute`` and ``second`` arguments default to zero and
    ``tzinfo`` defaults to :data:`None`.

    ``year``, ``month``, and ``day`` are passed to the constructor of
    :class:`.Date`. ``hour``, ``minute``, ``second``, ``nanosecond``, and
    ``tzinfo`` are passed to the constructor of :class:`.Time`. See their
    documentation for more details.

        >>> dt = DateTime(2018, 4, 30, 12, 34, 56, 789123456); dt
        neo4j.time.DateTime(2018, 4, 30, 12, 34, 56, 789123456)
        >>> dt.second
        56
    """

    __date: Date
    __time: Time

    # CONSTRUCTOR #

    def __new__(
        cls,
        year: int,
        month: int,
        day: int,
        hour: int = 0,
        minute: int = 0,
        second: int = 0,
        nanosecond: int = 0,
        tzinfo: _tzinfo | None = None,
    ) -> DateTime:
        return cls.combine(
            Date(year, month, day),
            Time(hour, minute, second, nanosecond, tzinfo),
        )

    # CLASS METHODS #

    @classmethod
    def now(cls, tz: _tzinfo | None = None) -> DateTime:
        """
        Get the current date and time.

        :param tz: timezone. Set to None to create a local :class:`.DateTime`.

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls._from_clock_time(_Clock().local_time(), UnixEpoch)
        else:
            utc_now = cls._from_clock_time(
                _Clock().utc_time(), UnixEpoch
            ).replace(tzinfo=tz)
            try:
                return tz.fromutc(utc_now)  # type: ignore
            except TypeError:
                # For timezone implementations not compatible with the custom
                # datetime implementations, we can't do better than this.
                utc_now_native = utc_now.to_native()
                now_native = tz.fromutc(utc_now_native)
                now = cls.from_native(now_native)
                return now.replace(
                    nanosecond=(
                        now.nanosecond
                        + utc_now.nanosecond
                        - utc_now_native.microsecond * 1000
                    )
                )

    @classmethod
    def utc_now(cls) -> DateTime:
        """Get the current date and time in UTC."""
        return cls._from_clock_time(_Clock().utc_time(), UnixEpoch)

    @classmethod
    def from_iso_format(cls, s) -> DateTime:
        """
        Parse a ISO formatted date with time string.

        :param s: String to parse

        :raises ValueError: if the string does not match the ISO format.
        """
        try:
            return cls.combine(
                Date.from_iso_format(s[0:10]), Time.from_iso_format(s[11:])
            )
        except ValueError as e:
            raise ValueError("DateTime string is not in ISO format") from e

    @classmethod
    def from_timestamp(
        cls, timestamp: float, tz: _tzinfo | None = None
    ) -> DateTime:
        """
        :class:`.DateTime` from a time stamp (seconds since unix epoch).

        :param timestamp: the unix timestamp (seconds since unix epoch).
        :param tz: timezone. Set to None to create a local :class:`.DateTime`.

        :raises OverflowError: if the timestamp is out of the range of values
            supported by the platform C localtime() function. It’s common for
            this to be restricted to years from 1970 through 2038.
        """
        if tz is None:
            return cls._from_clock_time(
                _ClockTime(timestamp) + _Clock().local_offset(), UnixEpoch
            )
        else:
            return (
                cls.utc_from_timestamp(timestamp)
                .replace(tzinfo=_timezone.utc)
                .astimezone(tz)
            )

    @classmethod
    def utc_from_timestamp(cls, timestamp: float) -> DateTime:
        """
        :class:`.DateTime` from a time stamp (seconds since unix epoch).

        Returns the :class:`.DateTime` as local date :class:`.DateTime` in UTC.
        """
        return cls._from_clock_time((timestamp, 0), UnixEpoch)

    @classmethod
    def from_ordinal(cls, ordinal: int) -> DateTime:
        """
        :class:`.DateTime` from an ordinal.

        For more info about ordinals see :meth:`.Date.from_ordinal`.
        """
        return cls.combine(Date.from_ordinal(ordinal), Midnight)

    @classmethod
    def combine(  # type: ignore[override]
        cls, date: Date, time: Time
    ) -> DateTime:
        """
        Combine a :class:`.Date` and a :class:`.Time` to a :class:`DateTime`.

        :param date: the date
        :param time: the time

        :raises AssertionError: if the parameter types don't match.
        """
        assert isinstance(date, Date)
        assert isinstance(time, Time)
        return cls._combine(date, time)

    @classmethod
    def _combine(cls, date: Date, time: Time) -> DateTime:
        instance = object.__new__(cls)
        instance.__date = date
        instance.__time = time
        return instance

    @classmethod
    def parse(cls, date_string, format):
        raise NotImplementedError

    @classmethod
    def from_native(cls, dt: _datetime) -> DateTime:
        """
        Convert from a native Python :class:`datetime.datetime` value.

        :param dt: the datetime to convert
        """
        return cls.combine(
            Date.from_native(dt.date()), Time.from_native(dt.timetz())
        )

    # TODO: 7.0 - remove public alias (copy over docstring)
    @classmethod
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> DateTime:
        """
        Convert from a :class:`ClockTime` relative to a given epoch.

        :param clock_time: the clock time as :class:`.ClockTime` or as tuple of
            (seconds, nanoseconds)
        :param epoch: the epoch to which ``clock_time`` is relative

        :raises ValueError: if ``clock_time`` is invalid.

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return cls._from_clock_time(clock_time, epoch)

    @classmethod
    def _from_clock_time(
        cls,
        clock_time: ClockTime | tuple[float, int],
        epoch: DateTime,
    ) -> DateTime:
        try:
            seconds, nanoseconds = _ClockTime(*clock_time)
        except (TypeError, ValueError) as e:
            raise ValueError("Clock time must be a 2-tuple of (s, ns)") from e
        else:
            ordinal, seconds = divmod(seconds, 86400)
            ticks = epoch.time().ticks + seconds * _NANO_SECONDS + nanoseconds
            days, ticks = divmod(ticks, 86400 * _NANO_SECONDS)
            ordinal += days
            date_ = Date.from_ordinal(ordinal + epoch.date().to_ordinal())
            time_ = Time.from_ticks(ticks)
            return cls.combine(date_, time_)

    # CLASS METHOD ALIASES #

    if _t.TYPE_CHECKING:

        @classmethod
        def fromisoformat(cls, s) -> DateTime: ...

        @classmethod
        def fromordinal(cls, ordinal: int) -> DateTime: ...

        @classmethod
        def fromtimestamp(
            cls, timestamp: float, tz: _tzinfo | None = None
        ) -> DateTime: ...

        # alias of parse
        @classmethod
        def strptime(cls, date_string, format): ...

        # alias of now
        @classmethod
        def today(cls, tz: _tzinfo | None = None) -> DateTime: ...

        @classmethod
        def utcfromtimestamp(cls, timestamp: float) -> DateTime: ...

        @classmethod
        def utcnow(cls) -> DateTime: ...

    # CLASS ATTRIBUTES #

    min: _t.Final[DateTime] = None  # type: ignore
    """The earliest date time value possible."""

    max: _t.Final[DateTime] = None  # type: ignore
    """The latest date time value possible."""

    resolution: _t.Final[Duration] = None  # type: ignore
    """The minimum resolution supported."""

    # INSTANCE ATTRIBUTES #

    @property
    def year(self) -> int:
        """
        The year of the :class:`.DateTime`.

        See :attr:`.Date.year`.
        """
        return self.__date.year

    @property
    def month(self) -> int:
        """
        The year of the :class:`.DateTime`.

        See :attr:`.Date.year`.
        """
        return self.__date.month

    @property
    def day(self) -> int:
        """
        The day of the :class:`.DateTime`'s date.

        See :attr:`.Date.day`.
        """
        return self.__date.day

    @property
    def year_month_day(self) -> tuple[int, int, int]:
        """
        The year_month_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_month_day`.
        """
        return self.__date.year_month_day

    @property
    def year_week_day(self) -> tuple[int, int, int]:
        """
        The year_week_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_week_day`.
        """
        return self.__date.year_week_day

    @property
    def year_day(self) -> tuple[int, int]:
        """
        The year_day of the :class:`.DateTime`'s date.

        See :attr:`.Date.year_day`.
        """
        return self.__date.year_day

    @property
    def hour(self) -> int:
        """
        The hour of the :class:`.DateTime`'s time.

        See :attr:`.Time.hour`.
        """
        return self.__time.hour

    @property
    def minute(self) -> int:
        """
        The minute of the :class:`.DateTime`'s time.

        See :attr:`.Time.minute`.
        """
        return self.__time.minute

    @property
    def second(self) -> int:
        """
        The second of the :class:`.DateTime`'s time.

        See :attr:`.Time.second`.
        """
        return self.__time.second

    @property
    def nanosecond(self) -> int:
        """
        The nanosecond of the :class:`.DateTime`'s time.

        See :attr:`.Time.nanosecond`.
        """
        return self.__time.nanosecond

    @property
    def tzinfo(self) -> _tzinfo | None:
        """
        The tzinfo of the :class:`.DateTime`'s time.

        See :attr:`.Time.tzinfo`.
        """
        return self.__time.tzinfo

    @property
    def hour_minute_second_nanosecond(self) -> tuple[int, int, int, int]:
        """
        The hour_minute_second_nanosecond of the :class:`.DateTime`'s time.

        See :attr:`.Time.hour_minute_second_nanosecond`.
        """
        return self.__time.hour_minute_second_nanosecond

    # OPERATIONS #

    @_t.overload
    def _get_both_normalized(
        self, other: _datetime | DateTime, strict: _t.Literal[True] = True
    ) -> tuple[DateTime, DateTime | _datetime]: ...

    @_t.overload
    def _get_both_normalized(
        self, other: _datetime | DateTime, strict: _t.Literal[False]
    ) -> tuple[DateTime, DateTime | _datetime] | None: ...

    def _get_both_normalized(
        self, other: _datetime | DateTime, strict: bool = True
    ) -> tuple[DateTime, DateTime | _datetime] | None:
        if (self.utc_offset() is None) ^ (other.utcoffset() is None):
            if strict:
                raise TypeError(
                    "can't compare offset-naive and offset-aware datetimes"
                )
            else:
                return None
        self_norm = self
        utc_offset = self.utc_offset()
        if utc_offset is not None:
            self_norm -= utc_offset
        self_norm = self_norm.replace(tzinfo=None)
        other_norm = other
        utc_offset = other.utcoffset()
        if utc_offset is not None:
            other_norm -= utc_offset
        other_norm = other_norm.replace(tzinfo=None)
        return self_norm, other_norm

    def __hash__(self):
        if self.nanosecond % 1000 == 0:
            return hash(self.to_native())
        self_norm = self
        utc_offset = self.utc_offset()
        if utc_offset is not None:
            self_norm -= utc_offset
        return hash(self_norm.date()) ^ hash(self_norm.time())

    def __eq__(self, other: object) -> bool:
        """
        ``==`` comparison with another datetime.

        Accepts :class:`.DateTime` and :class:`datetime.datetime`.
        """
        if not isinstance(other, (_datetime, DateTime)):
            return NotImplemented
        if self.utc_offset() == other.utcoffset():
            return self.date() == other.date() and self.time() == other.time()
        normalized = self._get_both_normalized(other, strict=False)
        if normalized is None:
            return False
        self_norm, other_norm = normalized
        return self_norm == other_norm

    def __lt__(  # type: ignore[override]
        self, other: _datetime | DateTime
    ) -> bool:
        """
        ``<`` comparison with another datetime.

        Accepts :class:`.DateTime` and :class:`datetime.datetime`.
        """
        if not isinstance(other, (_datetime, DateTime)):
            return NotImplemented
        if self.utc_offset() == other.utcoffset():
            if self.date() == other.date():
                return self.time() < other.time()
            return self.date() < other.date()
        self_norm, other_norm = self._get_both_normalized(other)
        return (
            self_norm.date() < other_norm.date()
            or self_norm.time() < other_norm.time()
        )

    def __le__(  # type: ignore[override]
        self, other: _datetime | DateTime
    ) -> bool:
        """
        ``<=`` comparison with another datetime.

        Accepts :class:`.DateTime` and :class:`datetime.datetime`.
        """
        if not isinstance(other, (_datetime, DateTime)):
            return NotImplemented
        if self.utc_offset() == other.utcoffset():
            if self.date() == other.date():
                return self.time() <= other.time()
            return self.date() <= other.date()
        self_norm, other_norm = self._get_both_normalized(other)
        return self_norm <= other_norm

    def __ge__(  # type: ignore[override]
        self, other: _datetime | DateTime
    ) -> bool:
        """
        ``>=`` comparison with another datetime.

        Accepts :class:`.DateTime` and :class:`datetime.datetime`.
        """
        if not isinstance(other, (_datetime, DateTime)):
            return NotImplemented
        if self.utc_offset() == other.utcoffset():
            if self.date() == other.date():
                return self.time() >= other.time()
            return self.date() >= other.date()
        self_norm, other_norm = self._get_both_normalized(other)
        return self_norm >= other_norm

    def __gt__(  # type: ignore[override]
        self, other: object
    ) -> bool:
        """
        ``>`` comparison with another datetime.

        Accepts :class:`.DateTime` and :class:`datetime.datetime`.
        """
        if not isinstance(other, (_datetime, DateTime)):
            return NotImplemented
        if self.utc_offset() == other.utcoffset():
            if self.date() == other.date():
                return self.time() > other.time()
            return self.date() > other.date()
        self_norm, other_norm = self._get_both_normalized(other)
        return (
            self_norm.date() > other_norm.date()
            or self_norm.time() > other_norm.time()
        )

    def __add__(self, other: _timedelta | Duration) -> DateTime:
        """Add a :class:`datetime.timedelta`."""
        if isinstance(other, Duration):
            if other == (0, 0, 0, 0):
                return self
            t = self.time()._to_clock_time() + _ClockTime(
                other.seconds, other.nanoseconds
            )
            days, seconds = _symmetric_divmod(t.seconds, 86400)
            date_ = self.date() + Duration(
                months=other.months, days=days + other.days
            )
            time_ = Time.from_ticks(seconds * _NANO_SECONDS + t.nanoseconds)
            return self.combine(date_, time_).replace(tzinfo=self.tzinfo)
        if isinstance(other, _timedelta):
            if other.total_seconds() == 0:
                return self
            t = self._to_clock_time() + _ClockTime(
                86400 * other.days + other.seconds,
                other.microseconds * 1000,
            )
            days, seconds = _symmetric_divmod(t.seconds, 86400)
            date_ = Date.from_ordinal(days + 1)
            time_ = Time.from_ticks(
                _round_half_to_even(seconds * _NANO_SECONDS + t.nanoseconds)
            )
            return self.combine(date_, time_).replace(tzinfo=self.tzinfo)
        return NotImplemented

    @_t.overload  # type: ignore[override]
    def __sub__(self, other: DateTime) -> Duration: ...

    @_t.overload
    def __sub__(self, other: _datetime) -> _timedelta: ...

    @_t.overload
    def __sub__(self, other: Duration | _timedelta) -> DateTime: ...

    def __sub__(self, other):
        """
        Subtract a datetime/DateTime or a timedelta/Duration.

        Subtracting a :class:`.DateTime` yields the duration between the two
        as a :class:`.Duration`.

        Subtracting a :class:`datetime.datetime` yields the duration between
        the two as a :class:`datetime.timedelta`.

        Subtracting a :class:`datetime.timedelta` or a :class:`.Duration`
        yields the :class:`.DateTime` that's the given duration away.
        """
        if isinstance(other, DateTime):
            self_month_ordinal = 12 * (self.year - 1) + self.month
            other_month_ordinal = 12 * (other.year - 1) + other.month
            months = self_month_ordinal - other_month_ordinal
            days = self.day - other.day
            t = self.time()._to_clock_time() - other.time()._to_clock_time()
            return Duration(
                months=months,
                days=days,
                seconds=t.seconds,
                nanoseconds=t.nanoseconds,
            )
        if isinstance(other, _datetime):
            days = self.to_ordinal() - other.toordinal()
            t = self.time()._to_clock_time() - _ClockTime(
                3600 * other.hour + 60 * other.minute + other.second,
                other.microsecond * 1000,
            )
            return _timedelta(
                days=days,
                seconds=t.seconds,
                microseconds=(t.nanoseconds // 1000),
            )
        if isinstance(other, Duration):
            return self.__add__(-other)
        if isinstance(other, _timedelta):
            return self.__add__(-other)
        return NotImplemented

    def __reduce__(self):
        return type(self)._restore, (self.__dict__,)

    @classmethod
    def _restore(cls, dict_):
        instance = object.__new__(cls)
        if dict_:
            instance.__dict__.update(dict_)
        return instance

    # INSTANCE METHODS #

    def date(self) -> Date:
        """Get the date."""
        return self.__date

    def time(self) -> Time:
        """Get the time without timezone info."""
        return self.__time.replace(tzinfo=None)

    def timetz(self) -> Time:
        """Get the time with timezone info."""
        return self.__time

    if _t.TYPE_CHECKING:

        def replace(  # type: ignore[override]
            self,
            year: _t.SupportsIndex = ...,
            month: _t.SupportsIndex = ...,
            day: _t.SupportsIndex = ...,
            hour: _t.SupportsIndex = ...,
            minute: _t.SupportsIndex = ...,
            second: _t.SupportsIndex = ...,
            nanosecond: _t.SupportsIndex = ...,
            tzinfo: _tzinfo | None = ...,
            **kwargs: object,
        ) -> DateTime: ...

    else:

        def replace(self, **kwargs) -> DateTime:
            """
            Return a ``DateTime`` with one or more components replaced.

            See :meth:`.Date.replace` and :meth:`.Time.replace` for available
            arguments.
            """
            date_ = self.__date.replace(**kwargs)
            time_ = self.__time.replace(**kwargs)
            return self.combine(date_, time_)

    def as_timezone(self, tz: _tzinfo) -> DateTime:
        """
        Convert this :class:`.DateTime` to another timezone.

        :param tz: the new timezone

        :returns: the same object if ``tz`` is :data:``None``.
            Else, a new :class:`.DateTime` that's the same point in time but in
            a different timezone.
        """
        if self.tzinfo is None:
            return self
        offset = _t.cast(_timedelta, self.utcoffset())
        utc = (self - offset).replace(tzinfo=tz)
        try:
            return tz.fromutc(utc)  # type: ignore
        except TypeError:
            # For timezone implementations not compatible with the custom
            # datetime implementations, we can't do better than this.
            native_utc = utc.to_native()
            native_res = tz.fromutc(native_utc)
            res = self.from_native(native_res)
            ns = native_res.microsecond * 1000 + self.nanosecond % 1000
            return res.replace(nanosecond=ns)

    def utc_offset(self) -> _timedelta | None:
        """
        Get the date times utc offset.

        See :meth:`.Time.utc_offset`.
        """
        return self.__time._utc_offset(self)

    def dst(self) -> _timedelta | None:
        """
        Get the daylight saving time adjustment (DST).

        See :meth:`.Time.dst`.
        """
        return _dst(self.tzinfo, self)

    def tzname(self) -> str | None:
        """
        Get the timezone name.

        See :meth:`.Time.tzname`.
        """
        return _tz_name(self.tzinfo, self)

    def time_tuple(self):
        raise NotImplementedError

    def utc_time_tuple(self):
        raise NotImplementedError

    def to_ordinal(self) -> int:
        """
        Get the ordinal of the :class:`.DateTime`'s date.

        See :meth:`.Date.to_ordinal`
        """
        return self.__date.to_ordinal()

    # TODO: 7.0 - remove public alias (copy over docstring)
    @_deprecated(
        "ClockTime is an implementation detail. "
        "It and its related methods will be removed in a future version."
    )
    def to_clock_time(self) -> ClockTime:
        """
        Convert to :class:`.ClockTime`.

        The returned :class:`.ClockTime` is relative to :attr:`DateTime.min`.

        .. deprecated:: 6.0
            :class:`ClockTime` is an implementation detail.
            It and its related methods will be removed in a future version.
        """
        return self._to_clock_time()

    def _to_clock_time(self) -> ClockTime:
        ordinal_seconds = 86400 * (self.__date.to_ordinal() - 1)
        time_seconds, nanoseconds = divmod(self.__time.ticks, _NANO_SECONDS)
        return _ClockTime(ordinal_seconds + time_seconds, nanoseconds)

    def to_native(self) -> _datetime:
        """
        Convert to a native Python :class:`datetime.datetime` value.

        This conversion is lossy as the native time implementation only
        supports a resolution of microseconds instead of nanoseconds.
        """
        y, mo, d = self.year_month_day
        h, m, s, ns = self.hour_minute_second_nanosecond
        ms = int(ns / 1000)
        tz = self.tzinfo
        return _datetime(y, mo, d, h, m, s, ms, tz)

    def weekday(self) -> int:
        """
        Get the weekday.

        See :meth:`.Date.weekday`
        """
        return self.__date.weekday()

    def iso_weekday(self) -> int:
        """
        Get the ISO weekday.

        See :meth:`.Date.iso_weekday`
        """
        return self.__date.iso_weekday()

    def iso_calendar(self) -> tuple[int, int, int]:
        """
        Get date as ISO tuple.

        See :meth:`.Date.iso_calendar`
        """
        return self.__date.iso_calendar()

    def iso_format(self, sep: str = "T") -> str:
        """
        Return the :class:`.DateTime` as ISO formatted string.

        This method joins ``self.date().iso_format()`` (see
        :meth:`.Date.iso_format`) and ``self.timetz().iso_format()`` (see
        :meth:`.Time.iso_format`) with ``sep`` in between.

        :param sep: the separator between the formatted date and time.
        """
        s_date = self.date().iso_format()
        s_time = self.timetz().iso_format()
        s = f"{s_date}{sep}{s_time}"
        time_tz = self.timetz()
        offset = time_tz.utc_offset()
        if offset is not None:
            # the time component will have taken care of formatting the offset
            return s
        offset = self.utc_offset()
        if offset is not None:
            offset_hours, offset_minutes = divmod(
                int(offset.total_seconds() // 60), 60
            )
            s += f"{offset_hours:+03d}:{offset_minutes:02d}"
        return s

    def __repr__(self) -> str:
        fields: list[str] = [
            *map(repr, self.year_month_day),
            *map(repr, self.hour_minute_second_nanosecond),
        ]
        if self.tzinfo is not None:
            fields.append(f"tzinfo={self.tzinfo!r}")
        return f"neo4j.time.DateTime({', '.join(fields)})"

    def __str__(self) -> str:
        return self.iso_format()

    def __format__(self, format_spec):
        if not format_spec:
            return self.iso_format()
        format_spec = _FORMAT_F_REPLACE.sub(
            f"{self.__time.nanosecond:09}", format_spec
        )
        return self.to_native().__format__(format_spec)

    # INSTANCE METHOD ALIASES #

    def __getattr__(self, name):
        """
        Get attribute by name.

        Map standard library attribute names to local attribute names,
        for compatibility.
        """
        try:
            return {
                "astimezone": self.as_timezone,
                "isocalendar": self.iso_calendar,
                "isoformat": self.iso_format,
                "isoweekday": self.iso_weekday,
                "strftime": self.__format__,
                "toordinal": self.to_ordinal,
                "timetuple": self.time_tuple,
                "utcoffset": self.utc_offset,
                "utctimetuple": self.utc_time_tuple,
            }[name]
        except KeyError:
            raise AttributeError(
                f"DateTime has no attribute {name!r}"
            ) from None

    if _t.TYPE_CHECKING:

        def astimezone(  # type: ignore[override]
            self, tz: _tzinfo
        ) -> DateTime: ...

        def isocalendar(  # type: ignore[override]
            self,
        ) -> tuple[int, int, int]: ...

        def iso_format(self, sep: str = "T") -> str:  # type: ignore[override]
            ...

        isoweekday = iso_weekday
        strftime = __format__
        toordinal = to_ordinal
        timetuple = time_tuple
        utcoffset = utc_offset
        utctimetuple = utc_time_tuple


DateTime.min = DateTime.combine(Date.min, Time.min)  # type: ignore
DateTime.max = DateTime.combine(Date.max, Time.max)  # type: ignore
DateTime.resolution = Time.resolution  # type: ignore

#: A :class:`.DateTime` instance set to ``0000-00-00T00:00:00``.
#: This has a :class:`.Date` component equal to :attr:`ZeroDate` and a
Never = DateTime.combine(ZeroDate, Midnight)

#: A :class:`.DateTime` instance set to ``1970-01-01T00:00:00``.
UnixEpoch = DateTime(1970, 1, 1, 0, 0, 0)


_ClockTime = ClockTime

if not _t.TYPE_CHECKING:
    del ClockTime


def __getattr__(name):
    if name == "ClockTime":
        _deprecation_warn(
            "ClockTime is an implementation detail. It and its related "
            "methods will be removed in a future version.",
            stack_level=2,
        )
        return _ClockTime
    raise AttributeError(f"module {__name__} has no attribute {name}")
