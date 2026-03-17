# tn.py - functions for handling Egypt Tax Number numbers
# coding: utf-8
#
# Copyright (C) 2022 Leandro Regueiro
# Copyright (C) 2025 Arthur de Jong
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

"""Tax Registration Number (الرقم الضريبي, Egypt tax number).

This number consists of 9 digits, usually separated into three groups
using hyphens to make it easier to read, like XXX-XXX-XXX.

More information:

* https://emsp.mts.gov.eg:8181/EMDB-web/faces/authoritiesandcompanies/authority/website/SearchAuthority.xhtml?lang=en

>>> validate('100-531-385')
'100531385'
>>> validate('٣٣١-١٠٥-٢٦٨')
'331105268'
>>> validate('12345')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> validate('VV3456789')
Traceback (most recent call last):
    ...
InvalidFormat: ...
>>> format('100531385')
'100-531-385'
"""  # noqa: E501

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


_ARABIC_NUMBERS_MAP = {
    # Arabic-indic digits.
    '٠': '0',
    '١': '1',
    '٢': '2',
    '٣': '3',
    '٤': '4',
    '٥': '5',
    '٦': '6',
    '٧': '7',
    '٨': '8',
    '٩': '9',
    # Extended arabic-indic digits.
    '۰': '0',
    '۱': '1',
    '۲': '2',
    '۳': '3',
    '۴': '4',
    '۵': '5',
    '۶': '6',
    '۷': '7',
    '۸': '8',
    '۹': '9',
}


def compact(number: str) -> str:
    """Convert the number to the minimal representation.

    This strips the number of any valid separators and removes surrounding
    whitespace. It also converts arabic numbers.
    """
    return ''.join((_ARABIC_NUMBERS_MAP.get(c, c) for c in clean(number, ' -/').strip()))


def validate(number: str) -> str:
    """Check if the number is a valid Egypt Tax Number number.

    This checks the length and formatting.
    """
    number = compact(number)
    if not isdigits(number):
        raise InvalidFormat()
    if len(number) != 9:
        raise InvalidLength()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Egypt Tax Number number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return '-'.join([number[:3], number[3:-3], number[-3:]])
