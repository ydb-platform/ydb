# isni.py - functions for handling International Standard Name Identifiers
#
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

"""ISNI (International Standard Name Identifier).

The International Standard Name Identifier (ISNI) is used to identify
contributors to media content such as books, television programmes, and
newspaper articles. The number consists of 16 digits where the last character
is a check digit.

More information:

* https://en.wikipedia.org/wiki/International_Standard_Name_Identifier
* https://isni.org/

>>> validate('0000 0001 2281 955X')
'000000012281955X'
>>> validate('0000 0001 1111 955X')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('0000 0001 2281')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> format('000000012281955X')
'0000 0001 2281 955X'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.iso7064 import mod_11_2
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the ISNI to the minimal representation. This strips the number
    of any valid ISNI separators and removes surrounding whitespace."""
    return clean(number, ' -').strip().upper()


def validate(number: str) -> str:
    """Check if the number is a valid ISNI. This checks the length and
    whether the check digit is correct."""
    number = compact(number)
    if not isdigits(number[:-1]):
        raise InvalidFormat()
    if len(number) != 16:
        raise InvalidLength()
    mod_11_2.validate(number)
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid ISNI."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return ' '.join((number[:4], number[4:8], number[8:12], number[12:16]))
