# This program is free software; you can redistribute it and/or modify
# it under the terms of the (LGPL) GNU Lesser General Public License as
# published by the Free Software Foundation; either version 3 of the
# License, or (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Library Lesser General Public License for more details at
# ( http://www.gnu.org/licenses/lgpl.html ).
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA 02111-1307, USA.
# written by: Jurko Gospodnetić ( jurko.gospodnetic@pke.hr )
# based on code by: Glen Walker
# based on code by: Nathan Van Gheem ( vangheem@gmail.com )

"""Classes for conversion between XML dates and Python objects."""

from suds import UnicodeMixin

import datetime
import re
import time


_SNIPPET_DATE =  \
    r"(?P<year>\d{1,})-(?P<month>\d{1,2})-(?P<day>\d{1,2})"
_SNIPPET_TIME =  \
    r"(?P<hour>\d{1,2}):(?P<minute>[0-5]?[0-9]):(?P<second>[0-5]?[0-9])"  \
    r"(?:\.(?P<subsecond>\d+))?"
_SNIPPET_ZONE =  \
    r"(?:(?P<tz_sign>[-+])(?P<tz_hour>\d{1,2})"  \
    r"(?::(?P<tz_minute>[0-5]?[0-9]))?)"  \
    r"|(?P<tz_utc>[Zz])"

_PATTERN_DATE = r"^%s(?:%s)?$" % (_SNIPPET_DATE, _SNIPPET_ZONE)
_PATTERN_TIME = r"^%s(?:%s)?$" % (_SNIPPET_TIME, _SNIPPET_ZONE)
_PATTERN_DATETIME = r"^%s[T ]%s(?:%s)?$" % (_SNIPPET_DATE, _SNIPPET_TIME,
                                            _SNIPPET_ZONE)

_RE_DATE = re.compile(_PATTERN_DATE)
_RE_TIME = re.compile(_PATTERN_TIME)
_RE_DATETIME = re.compile(_PATTERN_DATETIME)


class Date(UnicodeMixin):
    """
    An XML date object supporting the xsd:date datatype.

    @ivar value: The object value.
    @type value: B{datetime}.I{date}

    """

    def __init__(self, value):
        """
        @param value: The date value of the object.
        @type value: (datetime.date|str)
        @raise ValueError: When I{value} is invalid.

        """
        if isinstance(value, datetime.datetime):
            self.value = value.date()
        elif isinstance(value, datetime.date):
            self.value = value
        elif isinstance(value, str):
            self.value = self.__parse(value)
        else:
            raise ValueError("invalid type for Date(): %s" % type(value))

    @staticmethod
    def __parse(value):
        """
        Parse the string date.

        Supports the subset of ISO8601 used by xsd:date, but is lenient with
        what is accepted, handling most reasonable syntax.

        Any timezone is parsed but ignored because a) it is meaningless without
        a time and b) B{datetime}.I{date} does not support timezone
        information.

        @param value: A date string.
        @type value: str
        @return: A date object.
        @rtype: B{datetime}.I{date}

        """
        match_result = _RE_DATE.match(value)
        if match_result is None:
            raise ValueError("date data has invalid format '%s'" % (value,))
        return _date_from_match(match_result)

    def __unicode__(self):
        return self.value.isoformat()


class DateTime(UnicodeMixin):
    """
    An XML datetime object supporting the xsd:dateTime datatype.

    @ivar value: The object value.
    @type value: B{datetime}.I{datetime}

    """

    def __init__(self, value):
        """
        @param value: The datetime value of the object.
        @type value: (datetime.datetime|str)
        @raise ValueError: When I{value} is invalid.

        """
        if isinstance(value, datetime.datetime):
            self.value = value
        elif isinstance(value, str):
            self.value = self.__parse(value)
        else:
            raise ValueError("invalid type for DateTime(): %s" % type(value))

    @staticmethod
    def __parse(value):
        """
        Parse the string datetime.

        Supports the subset of ISO8601 used by xsd:dateTime, but is lenient
        with what is accepted, handling most reasonable syntax.

        Subsecond information is rounded to microseconds due to a restriction
        in the python datetime.datetime/time implementation.

        @param value: A datetime string.
        @type value: str
        @return: A datetime object.
        @rtype: B{datetime}.I{datetime}

        """
        match_result = _RE_DATETIME.match(value)
        if match_result is None:
           raise ValueError("date data has invalid format '%s'" % (value,))

        date = _date_from_match(match_result)
        time, round_up = _time_from_match(match_result)
        tzinfo = _tzinfo_from_match(match_result)

        value = datetime.datetime.combine(date, time)
        value = value.replace(tzinfo=tzinfo)
        if round_up:
            value += datetime.timedelta(microseconds=1)
        return value

    def __unicode__(self):
        return self.value.isoformat()


class Time(UnicodeMixin):
    """
    An XML time object supporting the xsd:time datatype.

    @ivar value: The object value.
    @type value: B{datetime}.I{time}

    """

    def __init__(self, value):
        """
        @param value: The time value of the object.
        @type value: (datetime.time|str)
        @raise ValueError: When I{value} is invalid.

        """
        if isinstance(value, datetime.time):
            self.value = value
        elif isinstance(value, str):
            self.value = self.__parse(value)
        else:
            raise ValueError("invalid type for Time(): %s" % type(value))

    @staticmethod
    def __parse(value):
        """
        Parse the string date.

        Supports the subset of ISO8601 used by xsd:time, but is lenient with
        what is accepted, handling most reasonable syntax.

        Subsecond information is rounded to microseconds due to a restriction
        in the python datetime.time implementation.

        @param value: A time string.
        @type value: str
        @return: A time object.
        @rtype: B{datetime}.I{time}

        """
        match_result = _RE_TIME.match(value)
        if match_result is None:
           raise ValueError("date data has invalid format '%s'" % (value,))

        time, round_up = _time_from_match(match_result)
        tzinfo = _tzinfo_from_match(match_result)
        if round_up:
            time = _bump_up_time_by_microsecond(time)
        return time.replace(tzinfo=tzinfo)

    def __unicode__(self):
        return self.value.isoformat()


class FixedOffsetTimezone(datetime.tzinfo, UnicodeMixin):
    """
    A timezone with a fixed offset and no daylight savings adjustment.

    http://docs.python.org/library/datetime.html#datetime.tzinfo

    """

    def __init__(self, offset):
        """
        @param offset: The fixed offset of the timezone.
        @type offset: I{int} or B{datetime}.I{timedelta}

        """
        if type(offset) == int:
            offset = datetime.timedelta(hours=offset)
        elif type(offset) != datetime.timedelta:
            raise TypeError("timezone offset must be an int or "
                "datetime.timedelta")
        if offset.microseconds or (offset.seconds % 60 != 0):
            raise ValueError("timezone offset must have minute precision")
        self.__offset = offset

    def dst(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.dst

        """
        return datetime.timedelta(0)

    def utcoffset(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.utcoffset

        """
        return self.__offset

    def tzname(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.tzname

        """
        # total_seconds was introduced in Python 2.7
        if hasattr(self.__offset, "total_seconds"):
            total_seconds = self.__offset.total_seconds()
        else:
            total_seconds = (self.__offset.days * 24 * 60 * 60) + \
                            (self.__offset.seconds)

        hours = total_seconds // (60 * 60)
        total_seconds -= hours * 60 * 60

        minutes = total_seconds // 60
        total_seconds -= minutes * 60

        seconds = total_seconds // 1
        total_seconds -= seconds

        if seconds:
            return "%+03d:%02d:%02d" % (hours, minutes, seconds)
        return "%+03d:%02d" % (hours, minutes)

    def __unicode__(self):
        return "FixedOffsetTimezone %s" % (self.tzname(None),)


class UtcTimezone(FixedOffsetTimezone):
    """
    The UTC timezone.

    http://docs.python.org/library/datetime.html#datetime.tzinfo

    """

    def __init__(self):
        FixedOffsetTimezone.__init__(self, datetime.timedelta(0))

    def tzname(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.tzname

        """
        return "UTC"

    def __unicode__(self):
        return "UtcTimezone"


class LocalTimezone(datetime.tzinfo):
    """
    The local timezone of the operating system.

    http://docs.python.org/library/datetime.html#datetime.tzinfo

    """

    def __init__(self):
        self.__offset = datetime.timedelta(seconds=-time.timezone)
        self.__dst_offset = None
        if time.daylight:
            self.__dst_offset = datetime.timedelta(seconds=-time.altzone)

    def dst(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.dst

        """
        if self.__is_daylight_time(dt):
            return self.__dst_offset - self.__offset
        return datetime.timedelta(0)

    def tzname(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.tzname

        """
        if self.__is_daylight_time(dt):
            return time.tzname[1]
        return time.tzname[0]

    def utcoffset(self, dt):
        """
        http://docs.python.org/library/datetime.html#datetime.tzinfo.utcoffset

        """
        if self.__is_daylight_time(dt):
            return self.__dst_offset
        return self.__offset

    def __is_daylight_time(self, dt):
        if not time.daylight:
            return False
        time_tuple = dt.replace(tzinfo=None).timetuple()
        time_tuple = time.localtime(time.mktime(time_tuple))
        return time_tuple.tm_isdst > 0

    def __unicode__(self):
        dt = datetime.datetime.now()
        return "LocalTimezone %s offset: %s dst: %s" % (self.tzname(dt),
            self.utcoffset(dt), self.dst(dt))


def _bump_up_time_by_microsecond(time):
    """
    Helper function bumping up the given datetime.time by a microsecond,
    cycling around silently to 00:00:00.0 in case of an overflow.

    @param time: Time object.
    @type value: B{datetime}.I{time}
    @return: Time object.
    @rtype: B{datetime}.I{time}

    """
    dt = datetime.datetime(2000, 1, 1, time.hour, time.minute,
        time.second, time.microsecond)
    dt += datetime.timedelta(microseconds=1)
    return dt.time()


def _date_from_match(match_object):
    """
    Create a date object from a regular expression match.

    The regular expression match is expected to be from _RE_DATE or
    _RE_DATETIME.

    @param match_object: The regular expression match.
    @type value: B{re}.I{MatchObject}
    @return: A date object.
    @rtype: B{datetime}.I{date}

    """
    year = int(match_object.group("year"))
    month = int(match_object.group("month"))
    day = int(match_object.group("day"))
    return datetime.date(year, month, day)


def _time_from_match(match_object):
    """
    Create a time object from a regular expression match.

    Returns the time object and information whether the resulting time should
    be bumped up by one microsecond due to microsecond rounding.

    Subsecond information is rounded to microseconds due to a restriction in
    the python datetime.datetime/time implementation.

    The regular expression match is expected to be from _RE_DATETIME or
    _RE_TIME.

    @param match_object: The regular expression match.
    @type value: B{re}.I{MatchObject}
    @return: Time object + rounding flag.
    @rtype: tuple of B{datetime}.I{time} and bool

    """
    hour = int(match_object.group('hour'))
    minute = int(match_object.group('minute'))
    second = int(match_object.group('second'))
    subsecond = match_object.group('subsecond')

    round_up = False
    microsecond = 0
    if subsecond:
        round_up = len(subsecond) > 6 and int(subsecond[6]) >= 5
        subsecond = subsecond[:6]
        microsecond = int(subsecond + "0" * (6 - len(subsecond)))
    return datetime.time(hour, minute, second, microsecond), round_up


def _tzinfo_from_match(match_object):
    """
    Create a timezone information object from a regular expression match.

    The regular expression match is expected to be from _RE_DATE, _RE_DATETIME
    or _RE_TIME.

    @param match_object: The regular expression match.
    @type value: B{re}.I{MatchObject}
    @return: A timezone information object.
    @rtype: B{datetime}.I{tzinfo}

    """
    tz_utc = match_object.group("tz_utc")
    if tz_utc:
        return UtcTimezone()

    tz_sign = match_object.group("tz_sign")
    if not tz_sign:
        return

    h = int(match_object.group("tz_hour") or 0)
    m = int(match_object.group("tz_minute") or 0)
    if h == 0 and m == 0:
        return UtcTimezone()

    # Python limitation - timezone offsets larger than one day (in absolute)
    # will cause operations depending on tzinfo.utcoffset() to fail, e.g.
    # comparing two timezone aware datetime.datetime/time objects.
    if h >= 24:
        raise ValueError("timezone indicator too large")

    tz_delta = datetime.timedelta(hours=h, minutes=m)
    if tz_sign == "-":
        tz_delta *= -1
    return FixedOffsetTimezone(tz_delta)
