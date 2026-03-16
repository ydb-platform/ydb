"""
This module provides utility functions for type conversion.

Functions:
    - to_int: Convert a string to an integer with optional regular expression
    matching.
    - to_float: Convert a string to a float with optional regular expression
    matching.
    - to_unicode: Convert objects to Unicode strings.
    - to_str: Convert objects to byte strings.
    - scale_1024: Scale a number down to a suitable size based on powers of
    1024.
    - remap: Remap a value from one range to another.
"""

# Ignoring all mypy errors because mypy doesn't understand many modern typing
# constructs... please, use pyright instead if you can.
from __future__ import annotations

import decimal
import math
import re
import typing
from typing import Union

from . import types

_TN = types.TypeVar('_TN', bound=types.DecimalNumber)

_RegexpType: types.TypeAlias = Union[
    types.Pattern[str], str, types.Literal[True], None
]


def to_int(
    input_: str | None = None,
    default: int = 0,
    exception: types.ExceptionsType = (ValueError, TypeError),
    regexp: _RegexpType = None,
) -> int:
    r"""
    Convert the given input to an integer or return default.

    When trying to convert the exceptions given in the exception parameter
    are automatically catched and the default will be returned.

    The regexp parameter allows for a regular expression to find the digits
    in a string.
    When True it will automatically match any digit in the string.
    When a (regexp) object (has a search method) is given, that will be used.
    WHen a string is given, re.compile will be run over it first

    The last group of the regexp will be used as value

    >>> to_int('abc')
    0
    >>> to_int('1')
    1
    >>> to_int('')
    0
    >>> to_int()
    0
    >>> to_int('abc123')
    0
    >>> to_int('123abc')
    0
    >>> to_int('abc123', regexp=True)
    123
    >>> to_int('123abc', regexp=True)
    123
    >>> to_int('abc123abc', regexp=True)
    123
    >>> to_int('abc123abc456', regexp=True)
    123
    >>> to_int('abc123', regexp=re.compile(r'(\d+)'))
    123
    >>> to_int('123abc', regexp=re.compile(r'(\d+)'))
    123
    >>> to_int('abc123abc', regexp=re.compile(r'(\d+)'))
    123
    >>> to_int('abc123abc456', regexp=re.compile(r'(\d+)'))
    123
    >>> to_int('abc123', regexp=r'(\d+)')
    123
    >>> to_int('123abc', regexp=r'(\d+)')
    123
    >>> to_int('abc', regexp=r'(\d+)')
    0
    >>> to_int('abc123abc', regexp=r'(\d+)')
    123
    >>> to_int('abc123abc456', regexp=r'(\d+)')
    123
    >>> to_int('1234', default=1)
    1234
    >>> to_int('abc', default=1)
    1
    >>> to_int('abc', regexp=123)
    Traceback (most recent call last):
    ...
    TypeError: unknown argument for regexp parameter: 123
    """
    if regexp is True:
        regexp = re.compile(r'(\d+)')
    elif isinstance(regexp, str):
        regexp = re.compile(regexp)
    elif hasattr(regexp, 'search'):
        pass
    elif regexp is not None:
        raise TypeError(f'unknown argument for regexp parameter: {regexp!r}')

    try:
        if regexp and input_ and (match := regexp.search(input_)):
            input_ = match.groups()[-1]

        if input_ is None:
            return default
        else:
            return int(input_)
    except exception:
        return default


def to_float(
    input_: str,
    default: int = 0,
    exception: types.ExceptionsType = (ValueError, TypeError),
    regexp: _RegexpType = None,
) -> types.Number:
    r"""
    Convert the given `input_` to an integer or return default.

    When trying to convert the exceptions given in the exception parameter
    are automatically catched and the default will be returned.

    The regexp parameter allows for a regular expression to find the digits
    in a string.
    When True it will automatically match any digit in the string.
    When a (regexp) object (has a search method) is given, that will be used.
    When a string is given, re.compile will be run over it first

    The last group of the regexp will be used as value

    >>> '%.2f' % to_float('abc')
    '0.00'
    >>> '%.2f' % to_float('1')
    '1.00'
    >>> '%.2f' % to_float('abc123.456', regexp=True)
    '123.46'
    >>> '%.2f' % to_float('abc123', regexp=True)
    '123.00'
    >>> '%.2f' % to_float('abc0.456', regexp=True)
    '0.46'
    >>> '%.2f' % to_float('abc123.456', regexp=re.compile(r'(\d+\.\d+)'))
    '123.46'
    >>> '%.2f' % to_float('123.456abc', regexp=re.compile(r'(\d+\.\d+)'))
    '123.46'
    >>> '%.2f' % to_float('abc123.46abc', regexp=re.compile(r'(\d+\.\d+)'))
    '123.46'
    >>> '%.2f' % to_float('abc123abc456', regexp=re.compile(r'(\d+(\.\d+|))'))
    '123.00'
    >>> '%.2f' % to_float('abc', regexp=r'(\d+)')
    '0.00'
    >>> '%.2f' % to_float('abc123', regexp=r'(\d+)')
    '123.00'
    >>> '%.2f' % to_float('123abc', regexp=r'(\d+)')
    '123.00'
    >>> '%.2f' % to_float('abc123abc', regexp=r'(\d+)')
    '123.00'
    >>> '%.2f' % to_float('abc123abc456', regexp=r'(\d+)')
    '123.00'
    >>> '%.2f' % to_float('1234', default=1)
    '1234.00'
    >>> '%.2f' % to_float('abc', default=1)
    '1.00'
    >>> '%.2f' % to_float('abc', regexp=123)
    Traceback (most recent call last):
    ...
    TypeError: unknown argument for regexp parameter
    """
    if regexp is True:
        regexp = re.compile(r'(\d+(\.\d+|))')
    elif isinstance(regexp, str):
        regexp = re.compile(regexp)
    elif hasattr(regexp, 'search'):
        pass
    elif regexp is not None:
        raise TypeError('unknown argument for regexp parameter')

    try:
        if regexp and (match := regexp.search(input_)):
            input_ = match.group(1)
        return float(input_)
    except exception:
        return default


def to_unicode(
    input_: types.StringTypes,
    encoding: str = 'utf-8',
    errors: str = 'replace',
) -> str:
    """Convert objects to unicode, if needed decodes string with the given
    encoding and errors settings.

    :rtype: str

    >>> to_unicode(b'a')
    'a'
    >>> to_unicode('a')
    'a'
    >>> to_unicode('a')
    'a'
    >>> class Foo(object):
    ...     __str__ = lambda s: 'a'
    >>> to_unicode(Foo())
    'a'
    >>> to_unicode(Foo)
    "<class 'python_utils.converters.Foo'>"
    """
    if isinstance(input_, bytes):
        input_ = input_.decode(encoding, errors)
    else:
        input_ = str(input_)
    return input_


def to_str(
    input_: types.StringTypes,
    encoding: str = 'utf-8',
    errors: str = 'replace',
) -> bytes:
    """Convert objects to string, encodes to the given encoding.

    :rtype: str

    >>> to_str('a')
    b'a'
    >>> to_str('a')
    b'a'
    >>> to_str(b'a')
    b'a'
    >>> class Foo(object):
    ...     __str__ = lambda s: 'a'
    >>> to_str(Foo())
    'a'
    >>> to_str(Foo)
    "<class 'python_utils.converters.Foo'>"
    """
    if not isinstance(input_, bytes):
        if not hasattr(input_, 'encode'):
            input_ = str(input_)

        input_ = input_.encode(encoding, errors)
    return input_


def scale_1024(
    x: types.Number,
    n_prefixes: int,
) -> types.Tuple[types.Number, types.Number]:
    """Scale a number down to a suitable size, based on powers of 1024.

    Returns the scaled number and the power of 1024 used.

    Use to format numbers of bytes to KiB, MiB, etc.

    >>> scale_1024(310, 3)
    (310.0, 0)
    >>> scale_1024(2048, 3)
    (2.0, 1)
    >>> scale_1024(0, 2)
    (0.0, 0)
    >>> scale_1024(0.5, 2)
    (0.5, 0)
    >>> scale_1024(1, 2)
    (1.0, 0)
    """
    if x <= 0:
        power = 0
    else:
        power = min(int(math.log(x, 2) / 10), n_prefixes - 1)
    scaled = float(x) / (2 ** (10 * power))
    return scaled, power


@typing.overload
def remap(
    value: decimal.Decimal,
    old_min: decimal.Decimal | float,
    old_max: decimal.Decimal | float,
    new_min: decimal.Decimal | float,
    new_max: decimal.Decimal | float,
) -> decimal.Decimal: ...


@typing.overload
def remap(
    value: decimal.Decimal | float,
    old_min: decimal.Decimal,
    old_max: decimal.Decimal | float,
    new_min: decimal.Decimal | float,
    new_max: decimal.Decimal | float,
) -> decimal.Decimal: ...


@typing.overload
def remap(
    value: decimal.Decimal | float,
    old_min: decimal.Decimal | float,
    old_max: decimal.Decimal,
    new_min: decimal.Decimal | float,
    new_max: decimal.Decimal | float,
) -> decimal.Decimal: ...


@typing.overload
def remap(
    value: decimal.Decimal | float,
    old_min: decimal.Decimal | float,
    old_max: decimal.Decimal | float,
    new_min: decimal.Decimal,
    new_max: decimal.Decimal | float,
) -> decimal.Decimal: ...


@typing.overload
def remap(
    value: decimal.Decimal | float,
    old_min: decimal.Decimal | float,
    old_max: decimal.Decimal | float,
    new_min: decimal.Decimal | float,
    new_max: decimal.Decimal,
) -> decimal.Decimal: ...


# Note that float captures both int and float types so we don't need to
# specify them separately
@typing.overload
def remap(
    value: float,
    old_min: float,
    old_max: float,
    new_min: float,
    new_max: float,
) -> float: ...


def remap(  # pyright: ignore[reportInconsistentOverload]
    value: _TN,
    old_min: _TN,
    old_max: _TN,
    new_min: _TN,
    new_max: _TN,
) -> _TN:
    """
    remap a value from one range into another.

    >>> remap(500, 0, 1000, 0, 100)
    50
    >>> remap(250.0, 0.0, 1000.0, 0.0, 100.0)
    25.0
    >>> remap(-75, -100, 0, -1000, 0)
    -750
    >>> remap(33, 0, 100, -500, 500)
    -170
    >>> remap(decimal.Decimal('250.0'), 0.0, 1000.0, 0.0, 100.0)
    Decimal('25.0')

    This is a great use case example. Take an AVR that has dB values the
    minimum being -80dB and the maximum being 10dB and you want to convert
    volume percent to the equilivint in that dB range

    >>> remap(46.0, 0.0, 100.0, -80.0, 10.0)
    -38.6

    I added using decimal.Decimal so floating point math errors can be avoided.
    Here is an example of a floating point math error
    >>> 0.1 + 0.1 + 0.1
    0.30000000000000004

    If floating point remaps need to be done my suggstion is to pass at least
    one parameter as a `decimal.Decimal`. This will ensure that the output
    from this function is accurate. I left passing `floats` for backwards
    compatability and there is no conversion done from float to
    `decimal.Decimal` unless one of the passed parameters has a type of
    `decimal.Decimal`. This will ensure that any existing code that uses this
    funtion will work exactly how it has in the past.

    Some edge cases to test
    >>> remap(1, 0, 0, 1, 2)
    Traceback (most recent call last):
    ...
    ValueError: Input range (0-0) is empty

    >>> remap(1, 1, 2, 0, 0)
    Traceback (most recent call last):
    ...
    ValueError: Output range (0-0) is empty

    Args:
        value (int, float, decimal.Decimal): Value to be converted.
        old_min (int, float, decimal.Decimal): Minimum of the range for the
            value that has been passed.
        old_max (int, float, decimal.Decimal): Maximum of the range for the
            value that has been passed.
        new_min (int, float, decimal.Decimal): The minimum of the new range.
        new_max (int, float, decimal.Decimal): The maximum of the new range.

    Returns: int, float, decimal.Decimal: Value that has been re-ranged. If
        any of the parameters passed is a `decimal.Decimal`, all of the
        parameters will be converted to `decimal.Decimal`. The same thing also
        happens if one of the parameters is a `float`. Otherwise, all
        parameters will get converted into an `int`. Technically, you can pass
        a `str` of an integer and it will get converted. The returned value
        type will be `decimal.Decimal` if any of the passed parameters are
        `decimal.Decimal`, the return type will be `float` if any of the
        passed parameters are a `float`, otherwise the returned type will be
        `int`.
    """
    type_: types.Type[types.DecimalNumber]
    if (
        isinstance(value, decimal.Decimal)
        or isinstance(old_min, decimal.Decimal)
        or isinstance(old_max, decimal.Decimal)
        or isinstance(new_min, decimal.Decimal)
        or isinstance(new_max, decimal.Decimal)
    ):
        type_ = decimal.Decimal
    elif (
        isinstance(value, float)
        or isinstance(old_min, float)
        or isinstance(old_max, float)
        or isinstance(new_min, float)
        or isinstance(new_max, float)
    ):
        type_ = float
    else:
        type_ = int

    value = types.cast(_TN, type_(value))
    old_min = types.cast(_TN, type_(old_min))
    old_max = types.cast(_TN, type_(old_max))
    new_max = types.cast(_TN, type_(new_max))
    new_min = types.cast(_TN, type_(new_min))

    # These might not be floats but the Python type system doesn't understand
    # the generic type system in this case
    old_range = types.cast(float, old_max) - types.cast(float, old_min)
    new_range = types.cast(float, new_max) - types.cast(float, new_min)

    if old_range == 0:
        raise ValueError(f'Input range ({old_min}-{old_max}) is empty')

    if new_range == 0:
        raise ValueError(f'Output range ({new_min}-{new_max}) is empty')

    # The current state of Python typing makes it impossible to use the
    # generic type system in this case. Or so extremely verbose that it's not
    # worth it.
    new_value = (value - old_min) * new_range  # type: ignore[operator]  # pyright: ignore[reportOperatorIssue, reportUnknownVariableType]

    if type_ is int:
        new_value //= old_range  # pyright: ignore[reportUnknownVariableType]
    else:
        new_value /= old_range  # pyright: ignore[reportUnknownVariableType]

    new_value += new_min  # type: ignore[operator] # pyright: ignore[reportOperatorIssue, reportUnknownVariableType]

    return types.cast(_TN, new_value)
