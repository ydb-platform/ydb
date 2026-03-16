#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
from abc import abstractmethod
from collections.abc import Callable
import math
import operator
import datetime
from calendar import isleap
from decimal import Decimal, Context
from typing import cast, Any, TypeVar, Union

from elementpath.aliases import XPathParserType
from elementpath.helpers import MONTH_DAYS_LEAP, MONTH_DAYS, DAYS_IN_4Y, \
    DAYS_IN_100Y, DAYS_IN_400Y, days_from_common_era, adjust_day, \
    normalized_seconds, months2days, round_number, LazyPattern
from .any_types import AnyAtomicType
from .untyped import UntypedAtomic

###
# Constants for base delta operations
_ZERO_DELTA = datetime.timedelta(0)
_1DAY_DELTA = datetime.timedelta(days=1)
_REF_DATETIME = datetime.datetime(1, 1, 1)
_MAX_OFFSET = datetime.timedelta(hours=14, minutes=0)
_MIN_OFFSET = datetime.timedelta(hours=-14, minutes=0)

__all__ = ['Timezone', 'AbstractDateTime', 'DateTime', 'GregorianDay',
           'GregorianMonth', 'GregorianYear', 'GregorianMonthDay',
           'Date', 'GregorianDay', 'GregorianMonthDay', 'Duration',
           'DayTimeDuration', 'YearMonthDuration', 'GregorianYearMonth',
           'GregorianYearMonth10', 'DateTime10', 'GregorianYear10', 'Time',
           'DateTimeStamp', 'Date10']


class Timezone(datetime.tzinfo):
    """
    A tzinfo implementation for XSD timezone offsets. Offsets must be specified
    between -14:00 and +14:00.

    :param offset: a timedelta instance or an XSD timezone formatted string.
    """
    def __init__(self, offset: datetime.timedelta) -> None:
        super().__init__()
        if not isinstance(offset, datetime.timedelta):
            raise TypeError("offset must be a datetime.timedelta")
        if offset < _MIN_OFFSET or offset > _MAX_OFFSET:
            raise ValueError("offset must be between -14:00 and +14:00")
        self.offset = offset

    @classmethod
    def fromstring(cls, text: str) -> 'Timezone':
        try:
            hours, minutes = text.strip().split(':')
            if hours.startswith('-'):
                return cls(datetime.timedelta(hours=int(hours), minutes=-int(minutes)))
            else:
                return cls(datetime.timedelta(hours=int(hours), minutes=int(minutes)))
        except AttributeError:
            raise TypeError("argument is not a string")
        except ValueError:
            if text.strip() == 'Z':
                return cls(datetime.timedelta(0))
            raise ValueError("%r: not an XSD timezone formatted string" % text) from None

    @classmethod
    def fromduration(cls, duration: 'Duration') -> 'Timezone':
        if duration.seconds % 60 != 0:
            raise ValueError("{!r} has not an integral number of minutes".format(duration))
        return cls(datetime.timedelta(seconds=int(duration.seconds)))

    def __getinitargs__(self) -> tuple[datetime.timedelta]:
        return self.offset,

    def __hash__(self) -> int:
        return hash(self.offset)

    def __eq__(self, other: object) -> bool:
        return isinstance(other, Timezone) and self.offset == other.offset

    def __ne__(self, other: object) -> bool:
        return not isinstance(other, Timezone) or self.offset != other.offset

    def __repr__(self) -> str:
        return "%s(%r)" % (self.__class__.__name__, self.offset)

    def __str__(self) -> str:
        return self.tzname(None)

    def utcoffset(self, dt: datetime.datetime | None) -> datetime.timedelta:
        if not isinstance(dt, datetime.datetime) and dt is not None:
            raise TypeError("utcoffset() argument must be a "
                            "datetime.datetime instance or None")
        return self.offset

    def tzname(self, dt: datetime.datetime | None) -> str:
        if not isinstance(dt, datetime.datetime) and dt is not None:
            raise TypeError("tzname() argument must be a "
                            "datetime.datetime instance or None")

        if not self.offset:
            return 'Z'
        elif self.offset < datetime.timedelta(0):
            sign, offset = '-', -self.offset
        else:
            sign, offset = '+', self.offset

        hours, minutes = offset.seconds // 3600, offset.seconds // 60 % 60
        return '{}{:02d}:{:02d}'.format(sign, hours, minutes)

    def dst(self, dt: datetime.datetime | None) -> None:
        if not isinstance(dt, datetime.datetime) and dt is not None:
            raise TypeError("dst() argument must be a "
                            "datetime.datetime instance or None")

    def fromutc(self, dt: datetime.datetime) -> datetime.datetime:
        if isinstance(dt, datetime.datetime):
            return dt + self.offset
        raise TypeError("fromutc() argument must be a datetime.datetime instance")


_UTC_TIMEZONE = Timezone(datetime.timedelta(0))


def get_comparable_datetimes(dt1: datetime.datetime, dt2: datetime.datetime) \
        -> tuple[datetime.datetime, datetime.datetime]:
    """
    Fill timezone because Python can't compare offset-naive and offset-aware datetimes.
    """
    if dt1.tzinfo is dt2.tzinfo:
        return dt1, dt2
    elif dt1.tzinfo is None:
        return dt1.replace(tzinfo=_UTC_TIMEZONE), dt2
    elif dt2.tzinfo is None:
        return dt1, dt2.replace(tzinfo=_UTC_TIMEZONE)
    else:
        return dt1, dt2


DT = TypeVar('DT', bound='AbstractDateTime')


class AbstractDateTime(AnyAtomicType):
    """
    A class for representing XSD date/time objects. It uses and internal datetime.datetime
    attribute and an integer attribute for processing BCE years or for years after 9999 CE.
    """
    pattern = LazyPattern(r'^$')

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'AbstractDateTime':
        match value:
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case datetime.datetime():
                return cls.fromdatetime(value)
            case _:
                return cls.fromstring(value)

    __slots__ = ('_dt', '_year')

    def __init__(self, year: int = 2000, month: int = 1, day: int = 1, hour: int = 0,
                 minute: int = 0, second: int = 0, microsecond: int = 0,
                 tzinfo: datetime.tzinfo | None = None) -> None:

        if hour == 24 and minute == second == microsecond == 0:
            hour = 0
            if year == 9999 and month == 12 and day == 31:
                delta = _ZERO_DELTA
                year = 10000
                month = 1
                day = 1
            else:
                delta = _1DAY_DELTA
                hour = 0
        else:
            delta = _ZERO_DELTA

        if 1 <= year <= 9999:
            self._dt = datetime.datetime(year, month, day, hour, minute,
                                         second, microsecond, tzinfo)
            if delta:
                self._dt += delta
            self._year = self._dt.year
        elif year == 0:
            raise ValueError('0 is an illegal value for year')
        elif not isinstance(year, int):
            raise TypeError("invalid type %r for year" % type(year))
        elif abs(year) > 2 ** 31:
            raise OverflowError("year overflow")
        else:
            self._year = year
            if isleap(year + bool(self._xsd_version != '1.0')):
                self._dt = datetime.datetime(4, month, day, hour, minute,
                                             second, microsecond, tzinfo)
            else:
                self._dt = datetime.datetime(6, month, day, hour, minute,
                                             second, microsecond, tzinfo)
            if delta:
                self._dt += delta

    def __repr__(self) -> str:
        fields = self.pattern.groupindex.keys()
        arg_string = ', '.join(
            str(getattr(self, k))
            for k in ['year', 'month', 'day', 'hour', 'minute'] if k in fields
        )
        if 'second' in fields:
            if self.microsecond:
                arg_string += ', %d.%06d' % (self.second, self.microsecond)
            else:
                arg_string += ', %d' % self.second

        if self.tzinfo is not None:
            arg_string += ', tzinfo=%r' % self.tzinfo
        return '%s(%s)' % (self.__class__.__name__, arg_string)

    @abstractmethod
    def __str__(self) -> str:
        raise NotImplementedError

    def __hash__(self) -> int:
        return hash((self._dt, self._year))

    def __eq__(self, other: object) -> bool:
        return self._compare(other, operator.eq)

    def __ne__(self, other: object) -> bool:
        return not self._compare(other, operator.eq)

    def __lt__(self, other: object) -> bool:
        return self._compare(other, operator.lt)

    def __le__(self, other: object) -> bool:
        return self._compare(other, operator.le)

    def __gt__(self, other: object) -> bool:
        return self._compare(other, operator.gt)

    def __ge__(self, other: object) -> bool:
        return self._compare(other, operator.ge)

    def __add__(self, other: object) -> Union['DayTimeDuration', 'AbstractDateTime']:
        if isinstance(other, AbstractDateTime):
            raise TypeError("wrong type %r for operand %r" % (type(other), other))
        return self._operation(other, operator.add)

    def __sub__(self, other: object) -> Union['DayTimeDuration', 'AbstractDateTime']:
        return self._operation(other, operator.sub)

    def _compare(self, other: object, op: Callable[[Any, Any], bool]) -> bool:
        if isinstance(other, datetime.datetime):
            dt, year = other, other.year
        elif isinstance(other, AbstractDateTime):
            match (self.name, other.name):
                case ('time', 'date') | ('date', 'time'):
                    if op is operator.eq:
                        raise TypeError("wrong type %r for operand %r" % (type(other), other))
                case ('dateTime', 'date') | ('date', 'dateTime'):
                    if op is not operator.eq:
                        raise TypeError("wrong type %r for operand %r" % (type(other), other))

            dt, year = other._dt, other._year
        elif op is operator.eq:
            return False
        else:
            raise TypeError("wrong type %r for operand %r" % (type(other), other))

        if self._year != year:
            return op(self._year, year)
        elif self._dt.tzinfo is dt.tzinfo:
            return op(self._dt, dt)
        elif self.tzinfo is None:
            return op(self._dt.replace(tzinfo=_UTC_TIMEZONE), dt)
        elif other.tzinfo is None:
            return op(self._dt, dt.replace(tzinfo=_UTC_TIMEZONE))
        else:
            return op(self._dt, dt)

    def _operation(self, other: object, op: Callable[[Any, Any], Any]) \
            -> Union['DayTimeDuration', 'AbstractDateTime']:
        match other:
            case AbstractDateTime():
                dt1, dt2 = get_comparable_datetimes(self._dt, other._dt)
                if 1 <= self._year <= 9999 and 1 <= other._year <= 9999:
                    return DayTimeDuration.fromtimedelta(dt1 - dt2)
                return DayTimeDuration.fromtimedelta(self.todelta() - other.todelta())

            case datetime.timedelta():
                delta = op(self.todelta(), other)
                return type(self).fromdelta(delta, adjust_timezone=True)

            case DayTimeDuration():
                delta = op(self.todelta(), other.get_timedelta())
                tzinfo = self._dt.tzinfo
                if tzinfo is None:
                    return type(self).fromdelta(delta)

                value = type(self).fromdelta(delta + tzinfo.utcoffset(None))
                value.tzinfo = tzinfo
                return value

            case YearMonthDuration():
                month = op(self._dt.month - 1, other.months) % 12 + 1
                year = self._year + op(self._dt.month - 1, other.months) // 12
                day = adjust_day(year, month, self._dt.day)

                if year > 0:
                    dt = self._dt.replace(year=year, month=month, day=day)
                elif isleap(year):
                    dt = self._dt.replace(year=4, month=month, day=day)
                else:
                    dt = self._dt.replace(year=6, month=month, day=day)

                kwargs = {k: getattr(dt, k) for k in self.pattern.groupindex.keys()}
                if year <= 0:
                    kwargs['year'] = year
                return type(self)(**kwargs)

            case _:
                raise TypeError("wrong type %r for operand %r" % (type(other), other))

    @property
    def year(self) -> int:
        return self._year

    @property
    def bce(self) -> bool:
        return self._year < 0

    @property
    def iso_year(self) -> str:
        """The ISO string representation of the year field."""
        year = self.year
        if -9999 <= year < -1:
            return '{:05}'.format(year if self._xsd_version == '1.0' else year + 1)
        elif year == -1:
            return '-0001' if self._xsd_version == '1.0' else '0000'
        elif 0 <= year <= 9999:
            return '{:04}'.format(year)
        else:
            return str(year)

    @property
    def month(self) -> int:
        return self._dt.month

    @property
    def day(self) -> int:
        return self._dt.day

    @property
    def hour(self) -> int:
        return self._dt.hour

    @property
    def minute(self) -> int:
        return self._dt.minute

    @property
    def second(self) -> int:
        return self._dt.second

    @property
    def microsecond(self) -> int:
        return self._dt.microsecond

    @property
    def tzinfo(self) -> datetime.tzinfo | None:
        return cast(Timezone, self._dt.tzinfo)

    @tzinfo.setter
    def tzinfo(self, tz: datetime.tzinfo | None) -> None:
        self._dt = self._dt.replace(tzinfo=tz)

    def tzname(self) -> str | None:
        return self._dt.tzname()

    def astimezone(self, tz: datetime.tzinfo | None = None) -> datetime.datetime:
        return self._dt.astimezone(tz)

    def isocalendar(self) -> tuple[int, int, int]:
        return cast(tuple[int, int, int], self._dt.isocalendar())

    @classmethod
    def fromstring(cls: type[DT], datetime_string: str,
                   tzinfo: datetime.tzinfo | None = None) -> DT:
        """
        Creates an XSD date/time instance from a string formatted value.

        :param datetime_string: a string containing an XSD formatted date/time specification.
        :param tzinfo: optional implicit timezone information (defaults to UTC).
        :return: an AbstractDateTime concrete subclass instance.
        """
        if not isinstance(datetime_string, str):
            msg = '1st argument has an invalid type {!r}'
            raise TypeError(msg.format(type(datetime_string)))
        elif tzinfo and not isinstance(tzinfo, Timezone):
            msg = '2nd argument has an invalid type {!r}'
            raise TypeError(msg.format(type(tzinfo)))

        match = cls.pattern.match(datetime_string.strip())
        if match is None:
            msg = 'Invalid datetime string {!r} for {!r}'
            raise ValueError(msg.format(datetime_string, cls))

        match_dict = match.groupdict()
        kwargs: dict[str, int] = {
            k: int(v) for k, v in match_dict.items() if k != 'tzinfo' and v is not None
        }

        if match_dict['tzinfo'] is not None:
            tzinfo = Timezone.fromstring(match_dict['tzinfo'])

        if 'microsecond' in kwargs:
            microseconds = match_dict['microsecond']
            if len(microseconds) != 6:
                microseconds += '0' * (6 - len(microseconds))
                kwargs['microsecond'] = int(microseconds[:6])

        if 'year' in kwargs:
            year_digits = match_dict['year'].lstrip('-')
            if year_digits.startswith('0') and len(year_digits) > 4:
                msg = "Invalid datetime string {!r} for {!r} (when year " \
                      "exceeds 4 digits leading zeroes are not allowed)"
                raise ValueError(msg.format(datetime_string, cls))

            if cls._xsd_version == '1.0':
                if kwargs['year'] == 0:
                    raise ValueError("year '0000' is an illegal value for XSD 1.0")
            elif kwargs['year'] <= 0:
                kwargs['year'] -= 1

        return cls(tzinfo=tzinfo, **kwargs)

    @classmethod
    def fromdatetime(cls: type[DT], dt: Union[datetime.datetime, datetime.date, datetime.time],
                     year: int | None = None) -> DT:
        """
        Creates an XSD date/time instance from a datetime.datetime/date/time instance.

        :param dt: the datetime, date or time instance that stores the XSD Date/Time value.
        :param year: if a year is provided the created instance refers to it and the \
        possibly present *dt.year* part is ignored.
        :return: an AbstractDateTime concrete subclass instance.
        """
        if not isinstance(dt, (datetime.datetime, datetime.date, datetime.time)):
            raise TypeError('1st argument has an invalid type %r' % type(dt))
        elif year is not None and not isinstance(year, int):
            raise TypeError('2nd argument has an invalid type %r' % type(year))

        kwargs = {k: getattr(dt, k) for k in cls.pattern.groupindex.keys() if hasattr(dt, k)}
        if year is not None:
            kwargs['year'] = year
        return cls(**kwargs)

    @classmethod
    def fromdelta(cls, delta: datetime.timedelta, adjust_timezone: bool = False) \
            -> 'AbstractDateTime':
        """
        Creates an XSD dateTime/date instance from a datetime.timedelta related to
        0001-01-01T00:00:00 CE. In case of a date the time part is not counted.

        :param delta: a datetime.timedelta instance.
        :param adjust_timezone: if `True` adjusts the timezone of Date objects \
        with eventually present hours and minutes.
        """
        try:
            dt = _REF_DATETIME + delta
        except OverflowError:
            days = delta.days
            if days > 0:
                y400, days = divmod(days, DAYS_IN_400Y)
                y100, days = divmod(days, DAYS_IN_100Y)
                y4, days = divmod(days, DAYS_IN_4Y)
                y1, days = divmod(days, 365)
                year = y400 * 400 + y100 * 100 + y4 * 4 + y1 + 1
                if y1 == 4 or y100 == 4:
                    year -= 1
                    days = 365

                td = datetime.timedelta(days=days, seconds=delta.seconds,
                                        microseconds=delta.microseconds)
                dt = datetime.datetime(4 if isleap(year) else 6, 1, 1) + td

            elif days >= -366:
                year = -1
                td = datetime.timedelta(days=days, seconds=delta.seconds,
                                        microseconds=delta.microseconds)
                dt = datetime.datetime(5, 1, 1) + td

            else:
                days = -days - 366
                y400, days = divmod(days, DAYS_IN_400Y)
                y100, days = divmod(days, DAYS_IN_100Y)
                y4, days = divmod(days, DAYS_IN_4Y)
                y1, days = divmod(days, 365)
                year = -y400 * 400 - y100 * 100 - y4 * 4 - y1 - 2
                if y1 == 4 or y100 == 4:
                    year += 1
                    days = 365

                td = datetime.timedelta(days=-days, seconds=delta.seconds,
                                        microseconds=delta.microseconds)
                if not td:
                    dt = datetime.datetime(4 if isleap(year + 1) else 6, 1, 1)
                    year += 1
                else:
                    dt = datetime.datetime(5 if isleap(year + 1) else 7, 1, 1) + td
        else:
            year = dt.year

        if issubclass(cls, Date):
            if adjust_timezone and (dt.hour or dt.minute):
                assert dt.tzinfo is None
                hour, minute = dt.hour, dt.minute

                if hour < 14 or hour == 14 and minute == 0:
                    tz = Timezone(datetime.timedelta(hours=-hour, minutes=-minute))
                    dt = dt.replace(tzinfo=tz)
                else:
                    tz = Timezone(datetime.timedelta(hours=-dt.hour + 24, minutes=-minute))
                    dt = dt.replace(tzinfo=tz)
                    dt += datetime.timedelta(days=1)

            return cls(year, dt.month, dt.day, tzinfo=dt.tzinfo)
        return cls(year, dt.month, dt.day, dt.hour, dt.minute,
                   dt.second, dt.microsecond, dt.tzinfo)

    def todelta(self) -> datetime.timedelta:
        """Returns the datetime.timedelta from 0001-01-01T00:00:00 CE."""
        if 1 <= self._year <= 9999:
            if self._dt.tzinfo is None:
                return self._dt - _REF_DATETIME
            else:
                return self._dt - _REF_DATETIME.replace(tzinfo=_UTC_TIMEZONE)

        year, dt = self.year, self._dt
        tzinfo = None if dt.tzinfo is None else _UTC_TIMEZONE

        if year > 0:
            m_days = MONTH_DAYS_LEAP if isleap(year) else MONTH_DAYS
            days = days_from_common_era(year - 1) + sum(m_days[m] for m in range(1, dt.month))
        else:
            m_days = MONTH_DAYS_LEAP if isleap(year + 1) else MONTH_DAYS
            days = days_from_common_era(year) + sum(m_days[m] for m in range(1, dt.month))

        delta = (dt - datetime.datetime(dt.year, dt.month, day=1, tzinfo=tzinfo))
        return datetime.timedelta(days=days, seconds=delta.total_seconds())


class DateTime(AbstractDateTime):
    name = 'dateTime'
    pattern = LazyPattern(
        r'^(?P<year>-?[0-9]*[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})'
        r'(T(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):'
        r'(?P<second>[0-9]{2})(?:\.(?P<microsecond>[0-9]+))?)'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )
    _xsd_version = '1.1'

    @classmethod
    def make(cls, value: Any,
             parser: XPathParserType | None = None,
             **kwargs: Any) -> 'DateTime':

        if cls._xsd_version == '1.0':
            dt_class = cls
        elif parser is not None:
            dt_class = DateTime10 if parser.xsd_version == '1.0' else cls
        elif kwargs.get('xsd_version') == '1.0':
            dt_class = DateTime10
        else:
            dt_class = cls

        match value:
            case UntypedAtomic():
                return dt_class.fromstring(value.value)
            case Date():
                return dt_class(value.year, value.month, value.day, tzinfo=value.tzinfo)
            case DateTime():
                if value.__class__ is dt_class:
                    return value

                year = value.year
                if year < 0:
                    if value.xsd_versions != '1.0':
                        year += 1
                    if not dt_class.__name__.endswith('10'):
                        year -= 1

                return dt_class(
                    year, value.month, value.day, value.hour, value.minute,
                    value.second, value.microsecond, value.tzinfo
                )
            case _:
                return dt_class.fromstring(value)

    def __init__(self, year: int, month: int, day: int,
                 hour: int = 0, minute: int = 0, second: int = 0,
                 microsecond: int = 0,
                 tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param year: the year, between -9999 and 9999
        :param month: the month, between 1 and 12
        :param day: the day, between 1 and 31
        :param hour: the hour, between 0 and 23
        :param minute: the minute, between 0 and 59
        :param second: the second, between 0 and 59
        :param microsecond: the microsecond, between 0 and 999999
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(year, month, day, hour, minute, second, microsecond, tzinfo)

    def __str__(self) -> str:
        if self.microsecond:
            return '{}-{:02}-{:02}T{:02}:{:02}:{:02}.{}{}'.format(
                self.iso_year, self.month, self.day, self.hour, self.minute, self.second,
                '{:06}'.format(self.microsecond).rstrip('0'), str(self.tzinfo or '')
            )
        return '{}-{:02}-{:02}T{:02}:{:02}:{:02}{}'.format(
            self.iso_year, self.month, self.day, self.hour,
            self.minute, self.second, str(self.tzinfo or '')
        )


class DateTime10(DateTime):
    name = 'dateTime'
    _xsd_version = '1.0'

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return subclass is DateTime10 or subclass is DateTime


class DateTimeStamp(DateTime):
    name = 'dateTimeStamp'
    pattern = LazyPattern(
        r'^(?P<year>-?[0-9]*[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})'
        r'(T(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):'
        r'(?P<second>[0-9]{2})(?:\.(?P<microsecond>[0-9]+))?)'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))$'
    )


class Date(AbstractDateTime):
    name = 'date'
    pattern = LazyPattern(
        r'^(?P<year>-?[0-9]*[0-9]{4})-(?P<month>[0-9]{2})-(?P<day>[0-9]{2})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )
    _xsd_version = '1.1'

    @classmethod
    def make(cls, value: Any,
             parser: XPathParserType | None = None,
             **kwargs: Any) -> 'Date':

        if cls._xsd_version == '1.0':
            dt_class = cls
        elif parser is not None:
            dt_class = Date10 if parser.xsd_version == '1.0' else Date
        elif kwargs.get('xsd_version') == '1.0':
            dt_class = Date10
        else:
            dt_class = cls

        match value:
            case UntypedAtomic():
                return dt_class.fromstring(value.value)
            case DateTime() | Date():
                if isinstance(value, Date) and value.__class__ is dt_class:
                    return value

                year = value.year
                if year < 0:
                    if value.xsd_versions != '1.0':
                        year += 1
                    if dt_class._xsd_version != '1.0':
                        year -= 1

                return dt_class(year, value.month, value.day, value.tzinfo)
            case _:
                return dt_class.fromstring(value)

    def __init__(self, year: int, month: int, day: int,
                 tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param year: the year, between -9999 and 9999
        :param month: the month, between 1 and 12
        :param day: the day, between 1 and 31
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(year, month, day, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '{}-{:02}-{:02}{}'.format(
            self.iso_year, self.month, self.day, str(self.tzinfo or '')
        )


class Date10(Date):
    name = 'date'
    _xsd_version = '1.0'

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return subclass is Date10 or subclass is Date


class GregorianDay(AbstractDateTime):
    name = 'gDay'
    pattern = LazyPattern(
        r'^---(?P<day>[0-9]{2})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'GregorianDay':
        match value:
            case GregorianDay():
                return value
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case Date() | DateTime():
                return cls(value.day, value.tzinfo)
            case _:
                return cls.fromstring(value)

    def __init__(self, day: int, tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param day: the day, between 1 and 31
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(day=day, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '---{:02}{}'.format(self.day, str(self.tzinfo or ''))


class GregorianMonth(AbstractDateTime):
    name = 'gMonth'
    pattern = LazyPattern(
        r'^--(?P<month>[0-9]{2})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'GregorianMonth':
        match value:
            case GregorianMonth():
                return value
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case Date() | DateTime():
                return cls(value.month, value.tzinfo)
            case _:
                return cls.fromstring(value)

    def __init__(self, month: int, tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param month: the month, between 1 and 12
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(month=month, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '--{:02}{}'.format(self.month, str(self.tzinfo or ''))


class GregorianMonthDay(AbstractDateTime):
    name = 'gMonthDay'
    pattern = LazyPattern(
        r'^--(?P<month>[0-9]{2})-(?P<day>[0-9]{2})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'GregorianMonthDay':
        match value:
            case GregorianMonthDay():
                return value
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case Date() | DateTime():
                return cls(value.month, value.day, value.tzinfo)
            case _:
                return cls.fromstring(value)

    def __init__(self, month: int, day: int, tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param month: the month, between 1 and 12
        :param day: the day, between 1 and 31
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(month=month, day=day, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '--{:02}-{:02}{}'.format(self.month, self.day, str(self.tzinfo or ''))


class GregorianYear(AbstractDateTime):
    name = 'gYear'
    pattern = LazyPattern(
        r'^(?P<year>-?[0-9]*[0-9]{4})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )
    _xsd_version = '1.1'

    @classmethod
    def make(cls, value: Any,
             parser: XPathParserType | None = None,
             **kwargs: Any) -> 'GregorianYear':

        if cls._xsd_version == '1.0':
            dt_class = cls
        elif parser is not None:
            dt_class = GregorianYear10 if parser.xsd_version == '1.0' else GregorianYear
        elif kwargs.get('xsd_version') == '1.0':
            dt_class = GregorianYear10
        else:
            dt_class = GregorianYear

        match value:
            case UntypedAtomic():
                return dt_class.fromstring(value.value)
            case Date() | DateTime() | GregorianYear():
                if value.__class__ is dt_class and isinstance(value, GregorianYear):
                    return value

                year = value.year
                if year < 0:
                    if value.xsd_versions != '1.0':
                        year += 1
                    if dt_class._xsd_version != '1.0':
                        year -= 1

                return dt_class(year, value.tzinfo)
            case _:
                return dt_class.fromstring(value)

    def __init__(self, year: int, tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param year: the year, between -9999 and 9999
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(year, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '{}{}'.format(self.iso_year, str(self.tzinfo or ''))


class GregorianYear10(GregorianYear):
    name = 'gYear'
    _xsd_version = '1.0'

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return subclass is GregorianYear10 or subclass is GregorianYear


class GregorianYearMonth(AbstractDateTime):
    name = 'gYearMonth'
    pattern = LazyPattern(
        r'^(?P<year>-?[0-9]*[0-9]{4})-(?P<month>[0-9]{2})'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )
    _xsd_version = '1.1'

    @classmethod
    def make(cls, value: Any,
             parser: XPathParserType | None = None,
             **kwargs: Any) -> 'GregorianYearMonth':

        if cls._xsd_version == '1.0':
            dt_class = cls
        elif parser is not None:
            dt_class = GregorianYearMonth10 if parser.xsd_version == '1.0' else GregorianYearMonth
        elif kwargs.get('xsd_version') == '1.0':
            dt_class = GregorianYearMonth10
        else:
            dt_class = GregorianYearMonth

        match value:
            case UntypedAtomic():
                return dt_class.fromstring(value.value)
            case Date() | DateTime() | GregorianYearMonth():
                if value.__class__ is dt_class and isinstance(value, GregorianYearMonth):
                    return value

                year = value.year
                if year < 0:
                    if value.xsd_versions != '1.0':
                        year += 1
                    if dt_class._xsd_version != '1.0':
                        year -= 1

                return dt_class(year, value.month, value.tzinfo)
            case _:
                return dt_class.fromstring(value)

    def __init__(self, year: int, month: int, tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param year: the year, between -9999 and 9999
        :param month: the month, between 1 and 12
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        super().__init__(year, month, tzinfo=tzinfo)

    def __str__(self) -> str:
        return '{}-{:02}{}'.format(self.iso_year, self.month, str(self.tzinfo or ''))


class GregorianYearMonth10(GregorianYearMonth):
    name = 'gYearMonth'
    _xsd_version = '1.0'

    @classmethod
    def __subclasshook__(cls, subclass: type) -> bool:
        return subclass is GregorianYearMonth10 or subclass is GregorianYearMonth


class Time(AbstractDateTime):
    name = 'time'
    pattern = LazyPattern(
        r'^(?P<hour>[0-9]{2}):(?P<minute>[0-9]{2}):'
        r'(?P<second>[0-9]{2})(?:\.(?P<microsecond>[0-9]+))?'
        r'(?P<tzinfo>Z|[+-](?:(?:0[0-9]|1[0-3]):[0-5][0-9]|14:00))?$'
    )

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'Time':
        match value:
            case Time():
                return value
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case DateTime():
                return cls(value.hour, value.minute, value.second,
                           value.microsecond, value.tzinfo)
            case _:
                return cls.fromstring(value)

    def __init__(self, hour: int = 0, minute: int = 0,
                 second: int = 0, microsecond: int = 0,
                 tzinfo: datetime.tzinfo | None = None) -> None:
        """
        :param hour: the hour, between 0 and 23
        :param minute: the minute, between 0 and 59
        :param second: the second, between 0 and 59
        :param microsecond: the microsecond, between 0 and 999999
        :param tzinfo: optional implicit timezone information (defaults to UTC)
        """
        if hour == 24 and minute == second == microsecond == 0:
            hour = 0
        super().__init__(
            hour=hour, minute=minute, second=second, microsecond=microsecond, tzinfo=tzinfo
        )

    def __str__(self) -> str:
        if self.microsecond:
            return '{:02}:{:02}:{:02}.{}{}'.format(
                self.hour, self.minute, self.second,
                '{:06}'.format(self.microsecond).rstrip('0'),
                str(self.tzinfo or '')
            )
        return '{:02}:{:02}:{:02}{}'.format(
            self.hour, self.minute, self.second, str(self.tzinfo or '')
        )

    def __add__(self, other: object) -> 'Time':
        if isinstance(other, DayTimeDuration):
            dt = self._dt + other.get_timedelta()
        elif isinstance(other, datetime.timedelta):
            dt = self._dt + other
        else:
            raise TypeError("wrong type %r for operand %r" % (type(other), other))
        return Time(dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)

    def __sub__(self, other: object) -> Union['DayTimeDuration', 'Time']:
        if isinstance(other, Time):
            dt1, dt2 = get_comparable_datetimes(self._dt, other._dt)
            return DayTimeDuration.fromtimedelta(dt1 - dt2)
        elif isinstance(other, DayTimeDuration):
            dt = self._dt - other.get_timedelta()
            return Time(dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)
        elif isinstance(other, datetime.timedelta):
            dt = self._dt - other
            return Time(dt.hour, dt.minute, dt.second, dt.microsecond, dt.tzinfo)
        else:
            raise TypeError("wrong type %r for operand %r" % (type(other), other))


_D = TypeVar('_D', bound='Duration')


class Duration(AnyAtomicType):
    name = 'duration'
    pattern = LazyPattern(
        r'^(-)?P(?=[0-9]|T)(?:([0-9]+)Y)?(?:([0-9]+)M)?(?:([0-9]+)D)?'
        r'(?:T(?=[0-9])(?:([0-9]+)H)?(?:([0-9]+)M)?(?:([0-9]+(?:\.[0-9]+)?)S)?)?$'
    )

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'Duration':
        match value:
            case Duration():
                return value
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case _:
                return cls.fromstring(value)

    __slots__ = ('months', 'seconds')

    def __init__(self, months: int = 0, seconds: Union[Decimal, int] = 0) -> None:
        """
        :param months: an integer value that represents years and months.
        :param seconds: a decimal or an integer instance that represents \
        days, hours, minutes, seconds and fractions of seconds.
        """
        if seconds < 0 < months or months < 0 < seconds:
            raise ValueError('signs differ: (months=%d, seconds=%d)' % (months, seconds))
        elif abs(months) > 2 ** 31:
            raise OverflowError("months duration overflow")
        elif abs(seconds) > 2 ** 63:  # type: ignore[operator]
            raise OverflowError("seconds duration overflow")

        self.months = months
        self.seconds = Decimal(seconds).quantize(Decimal('1.000000', context=Context(prec=30)))

    def __repr__(self) -> str:
        return '{}(months={!r}, seconds={})'.format(
            self.__class__.__name__, self.months, normalized_seconds(self.seconds)
        )

    def __str__(self) -> str:
        m = abs(self.months)
        years, months = m // 12, m % 12
        s = self.seconds.copy_abs()
        days = int(s // 86400)
        hours = int(s // 3600 % 24)
        minutes = int(s // 60 % 60)
        seconds = s % 60

        value = '-P' if self.sign else 'P'
        if years or months or days:
            if years:
                value += '%dY' % years
            if months:
                value += '%dM' % months
            if days:
                value += '%dD' % days

        if hours or minutes or seconds:
            value += 'T'
            if hours:
                value += '%dH' % hours
            if minutes:
                value += '%dM' % minutes
            if seconds:
                value += '%sS' % normalized_seconds(seconds)

        elif value[-1] == 'P':
            value += 'T0S'
        return value

    @classmethod
    def fromstring(cls: type[_D], text: str) -> _D:
        """
        Creates a Duration instance from a formatted XSD duration string.

        :param text: an ISO 8601 representation without week fragment and an optional decimal part \
        only for seconds fragment.
        """
        if not isinstance(text, str):
            msg = 'argument has an invalid type {!r}'
            raise TypeError(msg.format(type(text)))

        match = cls.pattern.match(text.strip())
        if match is None:
            raise ValueError('%r is not an xs:duration value' % text)

        sign, y, mo, d, h, mi, s = match.groups()
        seconds = Decimal(s or 0)
        minutes = int(mi or 0) + int(seconds // 60)
        seconds = seconds % 60
        hours = int(h or 0) + minutes // 60
        minutes = minutes % 60
        days = int(d or 0) + hours // 24
        hours = hours % 24
        months = int(mo or 0) + 12 * int(y or 0)

        if sign is None:
            seconds = seconds + (days * 24 + hours) * 3600 + minutes * 60
        else:
            months = -months
            seconds = -seconds - (days * 24 + hours) * 3600 - minutes * 60

        if cls is DayTimeDuration:
            if months:
                raise ValueError('months must be 0 for %r' % cls.__name__)
            return cls(seconds=seconds)
        elif cls is YearMonthDuration:
            if seconds:
                raise ValueError('seconds must be 0 for %r' % cls.__name__)
            return cls(months=months)
        return cls(months=months, seconds=seconds)

    @property
    def sign(self) -> str:
        return '-' if self.months < 0 or self.seconds < 0 else ''

    def _compare_durations(self, other: object, op: Callable[[Any, Any], Any]) -> bool:
        """
        Ordering is defined through comparison of four datetime.datetime values.

        Ref: https://www.w3.org/TR/2012/REC-xmlschema11-2-20120405/#duration
        """
        if not isinstance(other, self.__class__):
            raise TypeError("wrong type %r for operand %r" % (type(other), other))

        m1, s1 = self.months, int(self.seconds)
        m2, s2 = other.months, int(other.seconds)
        ms1, ms2 = int((self.seconds - s1) * 1000000), int((other.seconds - s2) * 1000000)
        return all([
            op(datetime.timedelta(months2days(1696, 9, m1), s1, ms1),
               datetime.timedelta(months2days(1696, 9, m2), s2, ms2)),
            op(datetime.timedelta(months2days(1697, 2, m1), s1, ms1),
               datetime.timedelta(months2days(1697, 2, m2), s2, ms2)),
            op(datetime.timedelta(months2days(1903, 3, m1), s1, ms1),
               datetime.timedelta(months2days(1903, 3, m2), s2, ms2)),
            op(datetime.timedelta(months2days(1903, 7, m1), s1, ms1),
               datetime.timedelta(months2days(1903, 7, m2), s2, ms2)),
        ])

    def __hash__(self) -> int:
        return hash((self.months, self.seconds))

    def __eq__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.months == other.months and self.seconds == other.seconds
        elif isinstance(other, UntypedAtomic):
            return self.__eq__(self.fromstring(other.value))
        else:
            return other == (self.months, self.seconds)

    def __ne__(self, other: object) -> bool:
        if isinstance(other, self.__class__):
            return self.months != other.months or self.seconds != other.seconds
        elif isinstance(other, UntypedAtomic):
            return self.__ne__(self.fromstring(other.value))
        else:
            return other != (self.months, self.seconds)

    def __lt__(self, other: object) -> bool:
        return self._compare_durations(other, operator.lt)

    def __le__(self, other: object) -> bool:
        return self == other or self._compare_durations(other, operator.le)

    def __gt__(self, other: object) -> bool:
        return self._compare_durations(other, operator.gt)

    def __ge__(self, other: object) -> bool:
        return self == other or self._compare_durations(other, operator.ge)


class YearMonthDuration(Duration):
    name = 'yearMonthDuration'

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'YearMonthDuration':
        match value:
            case YearMonthDuration():
                return value
            case Duration():
                return YearMonthDuration(months=value.months)
            case UntypedAtomic():
                return YearMonthDuration.fromstring(value.value)
            case _:
                return YearMonthDuration.fromstring(value)

    def __init__(self, months: int = 0) -> None:
        """
        :param months: an integer value that represents years and months.
        """
        super().__init__(months, 0)

    def __repr__(self) -> str:
        return '%s(months=%r)' % (self.__class__.__name__, self.months)

    def __str__(self) -> str:
        m = abs(self.months)
        years, months = m // 12, m % 12

        if not years:
            return '-P%dM' % months if self.months < 0 else 'P%dM' % months
        elif not months:
            return '-P%dY' % years if self.months < 0 else 'P%dY' % years
        elif self.months < 0:
            return '-P%dY%dM' % (years, months)
        else:
            return 'P%dY%dM' % (years, months)

    def __add__(self, other: object) \
            -> Union['YearMonthDuration', 'DayTimeDuration', 'AbstractDateTime']:
        if isinstance(other, self.__class__):
            return YearMonthDuration(months=self.months + other.months)
        elif isinstance(other, (DateTime, Date)):
            return other + self
        raise TypeError("cannot add %r to %r" % (type(other), type(self)))

    def __sub__(self, other: object) -> 'YearMonthDuration':
        if not isinstance(other, self.__class__):
            raise TypeError("cannot subtract %r from %r" % (type(other), type(self)))
        return YearMonthDuration(months=self.months - other.months)

    def __mul__(self, other: object) -> 'YearMonthDuration':
        if not isinstance(other, (float, int, Decimal)):
            raise TypeError("cannot multiply a %r by %r" % (type(self), type(other)))
        return YearMonthDuration(months=int(round_number(self.months * other)))

    def __truediv__(self, other: object) -> Union[float, 'YearMonthDuration']:
        if isinstance(other, self.__class__):
            return self.months / other.months
        elif isinstance(other, (float, int, Decimal)):
            return YearMonthDuration(months=int(round_number(self.months / other)))
        else:
            raise TypeError("cannot divide a %r by %r" % (type(self), type(other)))


class DayTimeDuration(Duration):
    name = 'dayTimeDuration'

    @classmethod
    def make(cls, value: Any, **kwargs: Any) -> 'DayTimeDuration':
        match value:
            case DayTimeDuration():
                return value
            case Duration():
                return cls(seconds=value.seconds)
            case UntypedAtomic():
                return cls.fromstring(value.value)
            case _:
                return cls.fromstring(value)

    @classmethod
    def fromtimedelta(cls, td: datetime.timedelta) -> 'DayTimeDuration':
        return cls(seconds=Decimal(
            '{}.{:06}'.format(td.days * 86400 + td.seconds, td.microseconds)
        ))

    def __init__(self, seconds: Union[Decimal, int] = 0) -> None:
        """
        :param seconds: a decimal or an integer instance that represents \
        days, hours, minutes, seconds and fractions of seconds.
        """
        super().__init__(0, seconds)

    def get_timedelta(self) -> datetime.timedelta:
        return datetime.timedelta(
            seconds=int(self.seconds), microseconds=int(self.seconds % 1 * 1000000)
        )

    def __repr__(self) -> str:
        return '%s(seconds=%s)' % (self.__class__.__name__, normalized_seconds(self.seconds))

    def __add__(self, other: object) -> Union['DayTimeDuration', Time, AbstractDateTime]:
        if isinstance(other, (Time, Date)):
            return other + self
        elif isinstance(other, self.__class__):
            return DayTimeDuration(self.seconds + other.seconds)
        raise TypeError("cannot add %r to %r" % (type(other), type(self)))

    def __sub__(self, other: object) -> 'DayTimeDuration':
        if not isinstance(other, self.__class__):
            raise TypeError("cannot subtract %r from %r" % (type(other), type(self)))
        return DayTimeDuration(seconds=self.seconds - other.seconds)

    def __mul__(self, other: object) -> 'DayTimeDuration':
        if isinstance(other, (float, int, Decimal)):
            if math.isnan(other):
                raise ValueError("cannot multiply a %r by NaN" % type(self))

            if isinstance(other, (int, Decimal)):
                seconds = self.seconds * other
            else:
                seconds = self.seconds * Decimal.from_float(other)

            return DayTimeDuration(seconds)
        else:
            raise TypeError("cannot multiply a %r by %r" % (type(self), type(other)))

    def __truediv__(self, other: object) -> Union[Decimal, 'DayTimeDuration']:
        if isinstance(other, self.__class__):
            return self.seconds / other.seconds
        elif isinstance(other, (float, int, Decimal)):
            if math.isnan(other):
                raise ValueError("cannot divide a %r by NaN" % type(self))

            if isinstance(other, (int, Decimal)):
                seconds = self.seconds / other
            else:
                seconds = self.seconds / Decimal.from_float(other)

            return DayTimeDuration(seconds)
        else:
            raise TypeError("cannot divide a %r by %r" % (type(self), type(other)))
