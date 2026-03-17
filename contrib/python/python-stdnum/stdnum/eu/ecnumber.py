# ecnumber.py - functions for handling European Community Numbers

# Copyright (C) 2023 Daniel Weber
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

"""EC Number (European Community number).

The EC Number is a unique seven-digit number assigned to chemical substances
for regulatory purposes within the European Union by the European Commission.

More information:

* https://en.wikipedia.org/wiki/European_Community_number

>>> validate('200-001-8')
'200-001-8'
>>> validate('200-001-9')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('20-0001-8')
Traceback (most recent call last):
    ...
InvalidFormat: ...
"""

from __future__ import annotations

import re

from stdnum.exceptions import *
from stdnum.util import clean


_ec_number_re = re.compile(r'^[0-9]{3}-[0-9]{3}-[0-9]$')


def compact(number: str) -> str:
    """Convert the number to the minimal representation."""
    number = clean(number, ' ').strip()
    if '-' not in number:
        number = '-'.join((number[:3], number[3:6], number[6:]))
    return number


def calc_check_digit(number: str) -> str:
    """Calculate the check digit for the number. The passed number should not
    have the check digit included."""
    number = compact(number).replace('-', '')
    return str(
        sum((i + 1) * int(n) for i, n in enumerate(number)) % 11)[0]


def validate(number: str) -> str:
    """Check if the number provided is a valid EC Number."""
    number = compact(number)
    if not len(number) == 9:
        raise InvalidLength()
    if not _ec_number_re.match(number):
        raise InvalidFormat()
    if number[-1] != calc_check_digit(number[:-1]):
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid EC Number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
