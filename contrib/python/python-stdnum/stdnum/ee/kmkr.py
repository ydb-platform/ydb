# kmkr.py - functions for handling Estonian VAT numbers
# coding: utf-8
#
# Copyright (C) 2012-2015 Arthur de Jong
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

"""KMKR (Käibemaksukohuslase, Estonian VAT number).

>>> compact('EE 100 931 558')
'100931558'
>>> validate('100594102')
'100594102'
>>> validate('100594103')  # incorrect check digit
Traceback (most recent call last):
    ...
InvalidChecksum: ...
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    number = clean(number, ' ').upper().strip()
    if number.startswith('EE'):
        number = number[2:]
    return number


def checksum(number: str) -> int:
    """Calculate the checksum."""
    weights = (3, 7, 1, 3, 7, 1, 3, 7, 1)
    return sum(w * int(n) for w, n in zip(weights, number)) % 10


def validate(number: str) -> str:
    """Check if the number provided is a valid VAT number. This checks the
    length, formatting and check digit."""
    number = compact(number)
    if not isdigits(number):
        raise InvalidFormat()
    if len(number) != 9:
        raise InvalidLength()
    if checksum(number) != 0:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid VAT number. This checks the
    length, formatting and check digit."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
