# ogm_vcs.py - functions for handling Belgian OGM-VCS
# coding: utf-8
#
# Copyright (C) 2025 Cédric Krier
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

"""Belgian OGM-VCS.

The OGM-VCS is used in bank transfer as structured communication.

* https://febelfin.be/en/publications/2023/febelfin-banking-standards-for-online-banking

>>> validate('+++010/8068/17183+++')
'010806817183'
>>> validate('010/8068/17180')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> format('010806817183')
'010/8068/17183'
>>> calc_check_digits('0108068171')
'83'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the number
    of any invalid separators and removes surrounding whitespace."""
    return clean(number, ' +/').strip()


def calc_check_digits(number: str) -> str:
    """Calculate the check digit that should be added."""
    number = compact(number)
    return '%02d' % ((int(number[:10]) % 97) or 97)


def validate(number: str) -> str:
    """Check if the number is a valid OGM-VCS."""
    number = compact(number)
    if not isdigits(number) or int(number) <= 0:
        raise InvalidFormat()
    if len(number) != 12:
        raise InvalidLength()
    if calc_check_digits(number) != number[-2:]:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid VAT number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Format the number provided for output."""
    number = compact(number)
    number = number.rjust(12, '0')
    return f'{number[:3]}/{number[3:7]}/{number[7:]}'
