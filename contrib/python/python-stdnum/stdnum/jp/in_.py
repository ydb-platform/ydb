# in_.py - functions for handling Japanese Individual Numbers (IN)
# coding: utf-8
#
# Copyright (C) 2025 Luca Sicurello
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

"""IN (個人番号, kojin bangō, Japanese Individual Number).

The Japanese Individual Number (個人番号, kojin bangō), often referred to as My
number (マイナンバー, mai nambā), is assigned to identify citizens and residents.
The number consists of 12 digits where the last digit is a check digit. No
personal information (such as name, gender, date of birth, etc.) is encoded
in the number.

More information:

 * https://en.wikipedia.org/wiki/Individual_Number
 * https://digitalforensic.jp/2016/03/14/column404/

>>> validate('6214 9832 0257')
'621498320257'
>>> validate('6214 9832 0258')
Traceback (most recent call last):
  ...
InvalidChecksum: ...
>>> validate('6214 9832 025X')
Traceback (most recent call last):
  ...
InvalidFormat: ...
>>> format('621498320257')
'6214 9832 0257'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, '- ').strip()


def calc_check_digit(number: str) -> str:
    """Calculate the check digit. The number passed should not have
    the check digit included."""
    weights = (6, 5, 4, 3, 2, 7, 6, 5, 4, 3, 2)
    s = sum(w * int(n) for w, n in zip(weights, number))
    return str(-s % 11 % 10)


def validate(number: str) -> str:
    """Check if the number is valid. This checks the length and check
    digit."""
    number = compact(number)
    if len(number) != 12:
        raise InvalidLength()
    if not isdigits(number):
        raise InvalidFormat()
    if calc_check_digit(number[:-1]) != number[-1]:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid IN."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the presentation format found on
    My Number Cards."""
    number = compact(number)
    return ' '.join(
        (number[0:4], number[4:8], number[8:12]))
