import datetime
import time

__version__ = "$Revision: 1.4 $"[11:-2]
__date__ = "$Date: 2007/02/11 08:57:17 $"[7:-2]


"""String utilities.

TODO:
  - allow strings in getDateTime routine;
"""
"""History (most recent first):
11-feb-2007 [als]   added INVALID_VALUE
10-feb-2007 [als]   allow date strings padded with spaces instead of zeroes
20-dec-2005 [yc]    handle long objects in getDate/getDateTime
16-dec-2005 [yc]    created from ``strutil`` module.
"""


def unzfill(str):
    """Return a string without ASCII NULs.

    This function searchers for the first NUL (ASCII 0) occurrence
    and truncates string till that position.

    """
    try:
        return str[:str.index(b'\0')]
    except ValueError:
        return str


def getDate(date=None):
    """Return `datetime.date` instance.

    Type of the ``date`` argument could be one of the following:
        None:
            use current date value;
        datetime.date:
            this value will be returned;
        datetime.datetime:
            the result of the date.date() will be returned;
        string:
            assuming "%Y%m%d" or "%y%m%dd" format;
        number:
            assuming it's a timestamp (returned for example
            by the time.time() call;
        sequence:
            assuming (year, month, day, ...) sequence;

    Additionally, if ``date`` has callable ``ticks`` attribute,
    it will be used and result of the called would be treated
    as a timestamp value.

    """
    if date is None:
        # use current value
        return datetime.date.today()
    if isinstance(date, datetime.date):
        return date
    if isinstance(date, datetime.datetime):
        return date.date()
    if isinstance(date, (int, float)):
        # date is a timestamp
        return datetime.date.fromtimestamp(date)
    if isinstance(date, str):
        date = date.replace(" ", "0")
        if len(date) == 6:
            # yymmdd
            return datetime.date(*time.strptime(date, "%y%m%d")[:3])
        # yyyymmdd
        return datetime.date(*time.strptime(date, "%Y%m%d")[:3])
    if hasattr(date, "__getitem__"):
        # a sequence (assuming date/time tuple)
        return datetime.date(*date[:3])
    return datetime.date.fromtimestamp(date.ticks())


def getDateTime(value=None):
    """Return `datetime.datetime` instance.

    Type of the ``value`` argument could be one of the following:
        None:
            use current date value;
        datetime.date:
            result will be converted to the `datetime.datetime` instance
            using midnight;
        datetime.datetime:
            ``value`` will be returned as is;
        string:
            *** CURRENTLY NOT SUPPORTED ***;
        number:
            assuming it's a timestamp (returned for example
            by the time.time() call;
        sequence:
            assuming (year, month, day, ...) sequence;

    Additionally, if ``value`` has callable ``ticks`` attribute,
    it will be used and result of the called would be treated
    as a timestamp value.

    """
    if value is None:
        # use current value
        return datetime.datetime.today()
    if isinstance(value, datetime.datetime):
        return value
    if isinstance(value, datetime.date):
        return datetime.datetime.fromordinal(value.toordinal())
    if isinstance(value, (int, float)):
        # value is a timestamp
        return datetime.datetime.fromtimestamp(value)
    if isinstance(value, str):
        raise NotImplementedError("Strings aren't currently implemented")
    if hasattr(value, "__getitem__"):
        # a sequence (assuming date/time tuple)
        return datetime.datetime(*tuple(value)[:6])
    return datetime.datetime.fromtimestamp(value.ticks())


class classproperty(property):
    """Works in the same way as a ``property``, but for the classes."""

    def __get__(self, obj, cls):
        return self.fget(cls)


class _InvalidValue:

    """Value returned from DBF records when field validation fails

    The value is not equal to anything except for itself
    and equal to all empty values: None, 0, empty string etc.
    In other words, invalid value is equal to None and not equal
    to None at the same time.

    This value yields zero upon explicit conversion to a number type,
    empty string for string types, and False for boolean.

    """

    def __eq__(self, other):
        return not other

    def __ne__(self, other):
        return other is not self

    def __bool__(self):
        return False

    def __int__(self):
        return 0
    __long__ = __int__

    def __float__(self):
        return 0.0

    def __str__(self):
        return ""

    def __repr__(self):
        return "<INVALID>"


# invalid value is a constant singleton
INVALID_VALUE = _InvalidValue()

# vim: set et sts=4 sw=4 :
