# identiteitskaartnummer.py - functions for handling Dutch passport numbers
# coding: utf-8
#
# Copyright (C) 2024 Jeff Horemans
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

"""Identiteitskaartnummer, Paspoortnummer (the Dutch passport number).

Each Dutch passport has an unique number of 9 alphanumerical characters.
The first 2 characters are always letters and the last character is a number.
The 6 characters in between can be either.

The letter "O" is never used to prevent it from being confused with the number "0".
Zeros are allowed, but are not used in numbers issued after December 2019.

More information:

* https://www.rvig.nl/node/356
* https://nl.wikipedia.org/wiki/Paspoort#Nederlands_paspoort

>>> compact('EM0000000')
'EM0000000'
>>> compact('XR 1001R5 8')
'XR1001R58'
>>> validate('EM0000000')
'EM0000000'
>>> validate('XR1001R58')
'XR1001R58'
>>> validate('XR1001R')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> validate('581001RXR')
Traceback (most recent call last):
    ...
InvalidFormat: ...
>>> validate('XR1O01R58')
Traceback (most recent call last):
    ...
InvalidComponent: ...
"""

from __future__ import annotations

import re

from stdnum.exceptions import *
from stdnum.util import clean


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding white space."""
    return clean(number, ' ').strip().upper()


def validate(number: str) -> str:
    """Check if the number is a valid passport number.
    This checks the length, formatting and check digit."""
    number = compact(number)
    if len(number) != 9:
        raise InvalidLength()
    if not re.match('^[A-Z]{2}[0-9A-Z]{6}[0-9]$', number):
        raise InvalidFormat()
    if 'O' in number:
        raise InvalidComponent(InvalidComponent("The letter 'O' is not allowed"))
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Dutch passport number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
