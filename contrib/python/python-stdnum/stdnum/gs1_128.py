# gs1_128.py - functions for handling GS1-128 codes
#
# Copyright (C) 2019 Sergi Almacellas Abellana
# Copyright (C) 2020-2025 Arthur de Jong
#
# This library is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License as published by the Free Software Foundation; either
# version 2.1 of the License, or (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
# Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public
# License along with this library; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA
# 02110-1301 USA

"""GS1-128 (Standard to encode product information in Code 128 barcodes).

The GS1-128 (also called EAN-128, UCC/EAN-128 or UCC-128) is an international
standard for embedding data such as best before dates, weights, etc. with
Application Identifiers (AI).

The GS1-128 standard is used as a product identification code on bar codes.
It embeds data with Application Identifiers (AI) that defines the kind of
data, the type and length. The standard is also known as UCC/EAN-128, UCC-128
and EAN-128.

GS1-128 is a subset of Code 128 symbology.

More information:

* https://en.wikipedia.org/wiki/GS1-128
* https://www.gs1.org/standards/barcodes/application-identifiers
* https://www.gs1.org/docs/barcodes/GS1_General_Specifications.pdf

>>> compact('(01)38425876095074(17)181119(37)1 ')
'013842587609507417181119371'
>>> encode({'01': '38425876095074'})
'0138425876095074'
>>> info('0138425876095074')
{'01': '38425876095074'}
>>> validate('(17)181119(01)38425876095074(37)1')
'013842587609507417181119371'
"""

from __future__ import annotations

import datetime
import decimal
import re

from stdnum import numdb
from stdnum.exceptions import *
from stdnum.util import clean


TYPE_CHECKING = False
if TYPE_CHECKING:  # pragma: no cover (only used when type checking)
    from collections.abc import Mapping
    from typing import Any


# our open copy of the application identifier database
_gs1_aidb = numdb.get('gs1_ai')


# Extra validation modules based on the application identifier
_ai_validators = {
    '01': 'stdnum.ean',
    '02': 'stdnum.ean',
    '8007': 'stdnum.iban',
}


def compact(number: str) -> str:
    """Convert the GS1-128 to the minimal representation.

    This strips the number of any valid separators and removes surrounding
    whitespace. For a more consistent compact representation use
    :func:`validate()`.
    """
    return clean(number, '()').strip()


def _encode_decimal(ai: str, fmt: str, value: object) -> tuple[str, str]:
    """Encode the specified decimal value given the format."""
    # For decimal types the last digit of the AI is used to encode the
    # number of decimal places (we replace the last digit)
    if isinstance(value, (list, tuple)) and fmt.startswith('N3+'):
        # Two numbers, where the number of decimal places is expected to apply
        # to the second value
        ai, number = _encode_decimal(ai, fmt[3:], value[1])
        return ai, str(value[0]).rjust(3, '0') + number
    value = str(value)
    if fmt.startswith('N..'):
        # Variable length number up to a certain length
        length = int(fmt[3:])
        value = value[:length + 1]
        number, decimals = (value.split('.') + [''])[:2]
        decimals = decimals[:9]
        return ai[:-1] + str(len(decimals)), number + decimals
    else:
        # Fixed length numeric
        length = int(fmt[1:])
        value = value[:length + 1]
        number, decimals = (value.split('.') + [''])[:2]
        decimals = decimals[:9]
        return ai[:-1] + str(len(decimals)), (number + decimals).rjust(length, '0')


def _encode_date(fmt: str, value: object) -> str:
    """Encode the specified date value given the format."""
    if isinstance(value, (list, tuple)) and fmt in ('N6..12', 'N6[+N6]'):
        # Two date values
        return '%s%s' % (
            _encode_date('N6', value[0]),
            _encode_date('N6', value[1]),
        )
    elif isinstance(value, datetime.date):
        # Format date in different formats
        if fmt in ('N6', 'N6..12', 'N6[+N6]'):
            return value.strftime('%y%m%d')
        elif fmt == 'N10':
            return value.strftime('%y%m%d%H%M')
        elif fmt in ('N6+N..4', 'N6[+N..4]', 'N6[+N4]'):
            value = value.strftime('%y%m%d%H%M')
            if value.endswith('00'):
                value = value[:-2]
            if value.endswith('00'):
                value = value[:-2]
            return value
        elif fmt in ('N8+N..4', 'N8[+N..4]'):
            value = value.strftime('%y%m%d%H%M%S')
            if value.endswith('00'):
                value = value[:-2]
            if value.endswith('00'):
                value = value[:-2]
            return value
        else:  # pragma: no cover (all formats should be covered)
            raise ValueError('unsupported format: %s' % fmt)
    else:
        # Value is assumed to be in the correct format already
        return str(value)


def _encode_value(ai: str, fmt: str, _type: str, value: object) -> tuple[str, str]:
    """Encode the specified value given the format and type."""
    if _type == 'decimal':
        return _encode_decimal(ai, fmt, value)
    elif _type == 'date':
        return ai, _encode_date(fmt, value)
    else:  # str or int types
        return ai, str(value)


def _max_length(fmt: str) -> int:
    """Determine the maximum length based on the format."""
    return sum(
        int(re.match(r'^[NXY][0-9]*?[.]*([0-9]+)[\[\]]?$', x).group(1))  # type: ignore[misc, union-attr]
        for x in fmt.split('+')
    )


def _pad_value(fmt: str, _type: str, value: str) -> str:
    """Pad the value to the maximum length for the format."""
    if _type in ('decimal', 'int'):
        return value.rjust(_max_length(fmt), '0')
    else:
        return value.ljust(_max_length(fmt))


def _decode_decimal(ai: str, fmt: str, value: str) -> decimal.Decimal | tuple[str, decimal.Decimal]:
    """Decode the specified decimal value given the fmt."""
    if fmt.startswith('N3+'):
        # If the number consists of two parts, it is assumed that the decimal
        # from the AI applies to the second part
        return (value[:3], _decode_decimal(ai, fmt[3:], value[3:]))  # type: ignore[return-value]
    decimals = int(ai[-1])
    if decimals:
        value = value[:-decimals] + '.' + value[-decimals:]
    return decimal.Decimal(value)


def _decode_date(fmt: str, value: str) -> datetime.date | datetime.datetime | tuple[datetime.date, datetime.date]:
    """Decode the specified date value given the fmt."""
    if len(value) == 6:
        if value[4:] == '00':
            # When day == '00', it must be interpreted as last day of month
            date = datetime.datetime.strptime(value[:4], '%y%m')
            if date.month == 12:
                date = date.replace(day=31)
            else:
                date = date.replace(month=date.month + 1, day=1) - datetime.timedelta(days=1)
            return date.date()
        else:
            return datetime.datetime.strptime(value, '%y%m%d').date()
    elif len(value) == 12 and fmt in ('N12', 'N6..12', 'N6[+N6]'):
        return (_decode_date('N6', value[:6]), _decode_date('N6', value[6:]))  # type: ignore[return-value]
    else:
        # Other lengths are interpreted as variable-length datetime values
        return datetime.datetime.strptime(value, '%y%m%d%H%M%S'[:len(value)])


def _decode_value(ai: str, fmt: str, _type: str, value: str) -> Any:
    """Decode the specified value given the fmt and type."""
    if _type == 'decimal':
        return _decode_decimal(ai, fmt, value)
    elif _type == 'date':
        return _decode_date(fmt, value)
    elif _type == 'int':
        return int(value)
    else:  # str
        return value.strip()


def info(number: str, separator: str = '') -> dict[str, Any]:
    """Return a dictionary containing the information from the GS1-128 code.

    The returned dictionary maps application identifiers to values with the
    appropriate type (`str`, `int`, `Decimal`, `datetime.date` or
    `datetime.datetime`).

    If a `separator` is provided it will be used as FNC1 to determine the end
    of variable-sized values.
    """
    number = compact(number)
    data = {}
    identifier = ''
    # skip separator
    if separator and number.startswith(separator):
        number = number[len(separator):]
    while number:
        # extract the application identifier
        ai, info = _gs1_aidb.info(number)[0]
        if not info or not number.startswith(ai):
            raise InvalidComponent()
        number = number[len(ai):]
        # figure out the value part
        value = number[:_max_length(info['format'])]
        if separator and info.get('fnc1'):
            idx = number.find(separator)
            if idx > 0:
                value = number[:idx]
        number = number[len(value):]
        # validate the value if we have a custom module for it
        if ai in _ai_validators:
            mod = __import__(_ai_validators[ai], globals(), locals(), ['validate'])
            mod.validate(value)
        # convert the number
        data[ai] = _decode_value(ai, info['format'], info['type'], value)
        # skip separator
        if separator and number.startswith(separator):
            number = number[len(separator):]
    return data


def encode(data: Mapping[str, object], separator: str = '', parentheses: bool = False) -> str:
    """Generate a GS1-128 for the application identifiers supplied.

    The provided dictionary is expected to map application identifiers to
    values. The supported value types and formats depend on the application
    identifier.

    If a `separator` is provided it will be used as FNC1 representation,
    otherwise variable-sized values will be expanded to their maximum size
    with appropriate padding.

    If `parentheses` is set the application identifiers will be surrounded
    by parentheses for readability.
    """
    ai_fmt = '(%s)' if parentheses else '%s'
    # we keep items sorted and keep fixed-sized values separate tot output
    # them first
    fixed_values = []
    variable_values = []
    for inputai, value in sorted(data.items()):
        ai, info = _gs1_aidb.info(str(inputai))[0]
        if not info:
            raise InvalidComponent()
        # validate the value if we have a custom module for it
        if ai in _ai_validators:
            mod = __import__(_ai_validators[ai], globals(), locals(), ['validate'])
            mod.validate(value)
        ai, value = _encode_value(ai, info['format'], info['type'], value)
        # store variable-sized values separate from fixed-size values
        if not info.get('fnc1'):
            fixed_values.append(ai_fmt % ai + value)
        else:
            variable_values.append((ai_fmt % ai, info['format'], info['type'], value))
    # we need the separator for all but the last variable-sized value
    # (or pad values if we don't have a separator)
    return ''.join(
        fixed_values + [
            ai + (value if separator else _pad_value(fmt, _type, value)) + separator
            for ai, fmt, _type, value in variable_values[:-1]
        ] + [
            ai + value
            for ai, fmt, _type, value in variable_values[-1:]
        ])


def validate(number: str, separator: str = '') -> str:
    """Check if the number provided is a valid GS1-128.

    This checks formatting of the number and values and returns a stable
    representation.

    If a separator is provided it will be used as FNC1 for both parsing the
    provided number and for encoding the returned number.
    """
    try:
        return encode(info(number, separator), separator)
    except ValidationError:
        raise
    except Exception:  # noqa: B902
        # We wrap all other exceptions to ensure that we only return
        # exceptions that are a subclass of ValidationError
        # (the info() and encode() functions expect some semblance of valid
        # input)
        raise InvalidFormat()


def is_valid(number: str, separator: str = '') -> bool:
    """Check if the number provided is a valid GS1-128."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
