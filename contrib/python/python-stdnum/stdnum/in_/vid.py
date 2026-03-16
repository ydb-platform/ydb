# vid.py - functions for handling Indian personal virtual identity numbers
#
# Copyright (C) 2024 Atul Deolekar
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

"""VID (Indian personal virtual identity number).

VID is a temporary, revocable 16-digit random number mapped with the Aadhaar number.
VID is used in lieu of Aadhaar number whenever authentication or e-KYC services
are performed.

VID is made up of 16 digits where the last digits is a check digit
calculated using the Verhoeff algorithm. The numbers are generated in a
random, non-repeating sequence and do not begin with 0 or 1.

More information:

* https://uidai.gov.in/en/contact-support/have-any-question/284-faqs/aadhaar-online-services/virtual-id-vid.html
* https://uidai.gov.in/images/resource/UIDAI_Circular_11012018.pdf

>>> validate('2341234123412341')
'2341234123412341'
>>> validate('2341234123412342')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('1341234123412341')  # number should not start with 0 or 1
Traceback (most recent call last):
    ...
InvalidFormat: ...
>>> validate('13412341234123')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> validate('2222222222222222')  # number cannot be a palindrome
Traceback (most recent call last):
    ...
InvalidFormat: ...
>>> format('2341234123412342')
'2341 2341 2341 2342'
>>> mask('2341234123412342')
'XXXX XXXX  XXXX 2342'
"""

from __future__ import annotations

import re

from stdnum import verhoeff
from stdnum.exceptions import *
from stdnum.util import clean


_vid_re = re.compile(r'^[2-9][0-9]{15}$')
"""Regular expression used to check syntax of VID numbers."""


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, ' -').strip()


def validate(number: str) -> str:
    """Check if the number provided is a valid VID number. This checks
    the length, formatting and check digit."""
    number = compact(number)
    if len(number) != 16:
        raise InvalidLength()
    if not _vid_re.match(number):
        raise InvalidFormat()
    if number == number[::-1]:
        raise InvalidFormat()  # VID cannot be a palindrome
    verhoeff.validate(number)
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid VID number. This checks
    the length, formatting and check digit."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return ' '.join((number[:4], number[4:8], number[8:12], number[12:]))


def mask(number: str) -> str:
    """Masks the first 8 digits as per Ministry of Electronics and
    Information Technology (MeitY) guidelines."""
    number = compact(number)
    return 'XXXX XXXX XXXX ' + number[-4:]
