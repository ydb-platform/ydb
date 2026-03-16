# bc_phn.py - functions for handling British Columbia Personal Health Numbers (PHNs)
# coding: utf-8
#
# Copyright (C) 2023 Ömer Boratav
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

"""BC PHN (British Columbia Personal Health Number).

A unique, numerical, lifetime identifier used in the specific identification
of an individual client or patient who has had any interaction with the
British Columbia health system. It is assigned only to and used by one person
and will not be assigned to any other person.

The existence of a PHN does not imply eligibility for health care services in
BC or provide any indication of an individual’s benefit status.

The PNH is a 10-digit number where the first digit is always 9, and the last
digit is a MOD-11 check digit.

More information:

* https://www2.gov.bc.ca/gov/content/health/health-drug-coverage/msp/bc-residents/personal-health-identification
* https://www2.gov.bc.ca/assets/gov/health/practitioner-pro/software-development-guidelines/conformance-standards/vol-4b-app-rules-client-registry.pdf


>>> validate('9698 658 215')
'9698658215'
>>> format('9698658215')
'9698 658 215'
>>> validate('9698648215')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('5736504210')
Traceback (most recent call last):
    ...
InvalidComponent: ...
>>> validate('9736A04212')
Traceback (most recent call last):
    ...
InvalidFormat: ...
"""  # noqa: E501

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, '- ').strip()


def calc_check_digit(number: str) -> str:
    """Calculate the check digit. The number passed should not have the check
    digit included."""
    weights = (2, 4, 8, 5, 10, 9, 7, 3)
    s = sum((w * int(n)) % 11 for w, n in zip(weights, number))
    return str((11 - s) % 11)


def validate(number: str) -> str:
    """Check if the number is a valid PHN. This checks the length,
    formatting and check digit."""
    number = compact(number)
    if len(number) != 10:
        raise InvalidLength()
    if not isdigits(number):
        raise InvalidFormat()
    if number[0] != '9':
        raise InvalidComponent()
    if number[9] != calc_check_digit(number[1:9]):
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid PHN."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return ' '.join((number[0:4], number[4:7], number[7:]))
