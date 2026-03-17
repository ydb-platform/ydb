#
# Copyright (c), 2018-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
import re
import math
import operator
from calendar import isleap, leapdays
from collections.abc import Mapping
from decimal import Decimal
from typing import Any, Generic, Optional, SupportsFloat, SupportsIndex, TypeVar, Union
from urllib.parse import urlsplit

###
# Common sets constants
OCCURRENCE_INDICATORS = frozenset(('?', '*', '+'))
BOOLEAN_VALUES = frozenset(('true', 'false', '1', '0'))
NUMERIC_INF_OR_NAN = frozenset(('INF', '-INF', '+INF', 'NaN'))
INVALID_NUMERIC = frozenset(
    ('inf', '+inf', '-inf', 'nan', 'infinity', '+infinity', '-infinity')
)

MathArgType = Union[SupportsFloat, SupportsIndex]
FloatArgType = Union[SupportsFloat, SupportsIndex, str]

T = TypeVar('T')


class Property(Generic[T]):
    """A descriptor for managing protected class properties."""
    __slots__ = ('_name', '_value')

    def __init__(self, value: T) -> None:
        self._value = value

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Any, owner: type[Any]) -> T:
        return self._value

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError("Can't set attribute {}".format(self._name))

    def __delete__(self, instance: Any) -> None:
        raise AttributeError("Can't delete attribute {}".format(self._name))

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}({self._value!r})'

    @property
    def value(self) -> T:
        return self._value


###
# Data validation patterns

class LazyPattern:
    """
    A descriptor for creating lazy regexp patterns. The compiled pattern is built
    only when the descriptor attribute is accessed (e.g. a hasattr() call).
    """
    _compiled: re.Pattern[str]

    __slots__ = ('_name', '_pattern', '_flags', '_compiled')

    def __init__(self, pattern: str, flags: Union[int, re.RegexFlag] = 0) -> None:
        self._pattern = pattern
        self._flags = flags

    def __set_name__(self, owner: type[Any], name: str) -> None:
        self._name = name

    def __get__(self, instance: Optional[Any], owner: type[Any]) -> re.Pattern[str]:
        try:
            return self._compiled
        except AttributeError:
            self._compiled = re.compile(self._pattern, self._flags)
            return self._compiled

    def __set__(self, instance: Any, value: Any) -> None:
        raise AttributeError("Can't set attribute {}".format(self._name))

    def __delete__(self, instance: Any) -> None:
        raise AttributeError("Can't delete attribute {}".format(self._name))

    @property
    def groupindex(self) -> Mapping[str, int] | None:
        try:
            return self._compiled.groupindex
        except AttributeError:
            self._compiled = re.compile(self._pattern, self._flags)
            return self._compiled.groupindex

    def match(self, string: str) -> re.Match[str] | None:
        try:
            return self._compiled.match(string)
        except AttributeError:
            self._compiled = re.compile(self._pattern, self._flags)
            return self._compiled.match(string)

    def search(self, string: str) -> re.Match[str] | None:
        try:
            return self._compiled.search(string)
        except AttributeError:
            self._compiled = re.compile(self._pattern, self._flags)
            return self._compiled.search(string)


class Patterns:
    """
    Helper patterns, the ones that aren't used at import time are defined lazy.
    """
    whitespaces = re.compile(r'[^\S\xa0]+')  # include ASCII 160 (non-breaking space)
    normalize = LazyPattern(r'[^\S\xa0]')
    ncname = LazyPattern(r'^[^\d\W][\w.\-\u00B7\u0300-\u036F\u203F\u2040]*$')
    extended_qname = LazyPattern(
        r'^(?:Q{(?P<namespace>[^}]+)}|'
        r'(?P<prefix>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*):)?'
        r'(?P<local>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*)$',
    )
    replacement = LazyPattern(r'^([^\\$]|\\{2}|\\\$|\$\d+)*$')
    sequence_type = LazyPattern(r'\s?([()?*+,])\s?')
    unicode_escape = LazyPattern(r'(?:\\u([0-9A-Fa-f]{4})|\\U([0-9A-Fa-f]{8}))')
    wrong_escape = LazyPattern(r'%(?![a-fA-F\d]{2})')
    xml_newlines = LazyPattern('\r\n|\r|\n')

    # Regex patterns related to names and namespaces
    namespace_uri = LazyPattern(r'{([^}]+)}')
    expanded_name = LazyPattern(
        r'^(?:(?:Q{|{)(?P<namespace>[^}]*)})?'
        r'(?P<local>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*)$',
    )
    unbound_expanded_name = LazyPattern(
        r'(?:Q{|{)([^}]*)}[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*'
    )
    unbound_qname = LazyPattern(
        r'(?:(?P<prefix>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*):)?'
        r'(?P<local>[^\d\W][\w\-.\u00B7\u0300-\u036F\u0387\u06DD\u06DE\u203F\u2040]*)',
    )


def upper_camel_case(s: str) -> str:
    return re.sub(r'^\d+', '', re.sub(r'[\W_]', '', s.title()))


def collapse_white_spaces(s: str) -> str:
    return Patterns.whitespaces.sub(' ', s).strip(' ')


def is_ncname(s: str) -> bool:
    return Patterns.ncname.match(s) is not None


def is_idrefs(value: Optional[str]) -> bool:
    return isinstance(value, str) and \
        all(Patterns.ncname.match(x) is not None for x in value.split())


###
# Operators
node_position = operator.attrgetter('position')


def reversed_sub(a: Any, b: Any) -> Any:
    return operator.sub(b, a)


def reversed_truediv(a: Any, b: Any) -> Any:
    return operator.truediv(b, a)


###
# Date/Time helpers
MONTH_DAYS = (0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)
MONTH_DAYS_LEAP = (0, 31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31)


def adjust_day(year: int, month: int, day: int) -> int:
    match month:
        case 1 | 3 | 5 | 7 | 8 | 10 | 12:
            return day
        case 4 | 6 | 9 | 11:
            return min(day, 30)
        case _:
            return min(day, 29) if isleap(year) else min(day, 28)


def days_from_common_era(year: int) -> int:
    """
    Returns the number of days from 0001-01-01 to the provided year. For a
    common era year the days are counted until the last day of December, for a
    BCE year the days are counted down from the end to the 1st of January.
    """
    match year:
        case year if year > 0:
            return year * 365 + year // 4 - year // 100 + year // 400
        case year if year >= -1:
            return year * 366
        case _:
            year = -year - 1
            return -(366 + year * 365 + year // 4 - year // 100 + year // 400)


DAYS_IN_4Y = days_from_common_era(4)
DAYS_IN_100Y = days_from_common_era(100)
DAYS_IN_400Y = days_from_common_era(400)


def months2days(year: int, month: int, months_delta: int) -> int:
    """
    Converts a delta of months to a delta of days, counting from the 1st day of the month,
    relative to the year and the month passed as arguments.

    :param year: the reference start year, a negative or zero value means a BCE year \
    (0 is 1 BCE, -1 is 2 BCE, -2 is 3 BCE, etc.).
    :param month: the starting month (1-12).
    :param months_delta: the number of months, if negative count backwards.
    """
    if not months_delta:
        return 0

    total_months = month - 1 + months_delta
    target_year = year + total_months // 12
    target_month = total_months % 12 + 1

    if month <= 2:
        y_days = 365 * (target_year - year) + leapdays(year, target_year)
    else:
        y_days = 365 * (target_year - year) + leapdays(year + 1, target_year + 1)

    months_days = MONTH_DAYS_LEAP if isleap(target_year) else MONTH_DAYS
    if target_month >= month:
        m_days = sum(months_days[m] for m in range(month, target_month))
        return y_days + m_days if y_days >= 0 else y_days + m_days
    else:
        m_days = sum(months_days[m] for m in range(target_month, month))
        return y_days - m_days if y_days >= 0 else y_days - m_days


def round_number(value: Union[float, int, Decimal]) -> Union[float, int, Decimal]:
    if math.isnan(value) or math.isinf(value):
        return value

    number = Decimal(value)
    if number > 0:
        return type(value)(number.quantize(Decimal('1'), rounding='ROUND_HALF_UP'))
    else:
        return type(value)(number.quantize(Decimal('1'), rounding='ROUND_HALF_DOWN'))


def normalized_seconds(seconds: Union[int, Decimal]) -> str:
    # Decimal.normalize() does not remove exp every time: eg. Decimal('1E+1')
    return '{:.6f}'.format(seconds).rstrip('0').rstrip('.')


def is_xml_codepoint(cp: int) -> bool:
    return cp in (0x9, 0xA, 0xD) or \
        0x20 <= cp <= 0xD7FF or \
        0xE000 <= cp <= 0xFFFD or \
        0x10000 <= cp <= 0x10FFFF


def ordinal(n: int) -> str:
    match n:
        case 11 | 12 | 13:
            return '%dth' % n
        case n if n % 10 == 1:
            return '%dst' % n
        case n if n % 10 == 2:
            return '%dnd' % n
        case n if n % 10 == 3:
            return '%drd' % n
        case _:
            return '%dth' % n


def get_double(value: FloatArgType, xsd_version: str | None = None) -> float:
    if isinstance(value, str):
        value = collapse_white_spaces(value)
        if value in NUMERIC_INF_OR_NAN and (xsd_version != '1.0' or value != '+INF'):
            if value == 'NaN':
                return math.nan  # for NaN use the predefined instance to keep identity
        elif value.lower() in INVALID_NUMERIC:
            raise ValueError(f'invalid value {value!r} for xs:double/xs:float')
    elif math.isnan(value):
        return math.nan

    return float(value)


def numeric_equal(op1: MathArgType, op2: MathArgType) -> bool:
    if op1 == op2:
        return True
    return math.isclose(op1, op2, rel_tol=1e-7, abs_tol=0.0)


def numeric_not_equal(op1: MathArgType, op2: MathArgType) -> bool:
    if op1 == op2:
        return False
    return not math.isclose(op1, op2, rel_tol=1e-7, abs_tol=0.0)


def equal(op1: Any, op2: Any) -> bool:
    if isinstance(op1, float) and math.isnan(op1):
        return isinstance(op2, float) and math.isnan(op2)
    return bool(op1 == op2)


def not_equal(op1: Any, op2: Any) -> bool:
    if isinstance(op1, float) and math.isnan(op1):
        return not isinstance(op2, float) or not math.isnan(op2)
    return bool(op1 != op2)


def match_wildcard(name: Optional[str], wildcard: str) -> bool:
    if not name:
        return False
    elif wildcard in ('*', '{*}*'):
        return True
    elif wildcard == '{}*':
        return name[0] != '{' or name[:2] == '{}'
    elif wildcard[-1] == '*':
        return name.startswith(wildcard[:-1])
    elif not wildcard.startswith('{*}'):
        return False
    elif name[0] == '{':
        return name.endswith(wildcard[2:])
    else:
        return name == wildcard[3:]


def escape_json_string(s: str, escaped: bool = False) -> str:
    if escaped:
        s = s.replace('\\"', '"')
    else:
        s = s.replace('\\', '\\\\')

    s = s.replace('\"', '\\"').\
        replace('\b', r'\b').\
        replace('\r', r'\r').\
        replace('\n', r'\n').\
        replace('\t', r'\t').\
        replace('\f', r'\f').\
        replace('/', r'\/')
    return ''.join(
        rf'\u{ord(x):04X}' if 1 <= ord(x) <= 31 or 127 <= ord(x) <= 159 else x
        for x in s
    )


def unescape_json_string(s: str) -> str:

    def unicode_escape_callback(match: re.Match[str]) -> str:
        group = match.group(1) or match.group(2)
        return chr(int(group.upper(), 16))

    s = s.replace('\\"', '\"').\
        replace(r'\b', '\b').\
        replace(r'\r', '\r').\
        replace(r'\n', '\n').\
        replace(r'\t', '\t').\
        replace(r'\f', '\f').\
        replace(r'\/', '/').\
        replace('\\\\', '\\')

    return Patterns.unicode_escape.sub(unicode_escape_callback, s)


def split_function_test(function_test: str) -> list[str]:
    if not function_test.startswith('function('):
        return []
    elif function_test == 'function(*)':
        return ['*']

    parts = function_test[9:].partition(') as ')
    if parts[0]:
        sequence_types = parts[0].split(', ')
        sequence_types.append(parts[2])
    else:
        sequence_types = [parts[2]]

    return sequence_types


def is_absolute_uri(uri: str) -> bool:
    try:
        parts = urlsplit(uri.strip())
    except ValueError:
        return False
    else:
        return parts.scheme == 'urn' or \
            parts.scheme != '' and parts.netloc != '' or \
            parts.path.startswith('/')
