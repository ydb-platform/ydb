# onrc.py - functions for handling Romanian ONRC numbers
# coding: utf-8
#
# Copyright (C) 2020-2024 Dimitrios Josef Moustos
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

"""ONRC (Ordine din Registrul Comerţului, Romanian Trade Register identifier).

All businesses in Romania have the to register with the National Trade
Register Office to receive a registration number. The number contains
information about the type of company and registration year.

On 2024-07-26 a new format was introduced and for a while both old and new
formats need to be valid.

More information:

* https://targetare.ro/blog/schimbari-importante-la-registrul-comertului-ce-trebuie-sa-stii-despre-noul-format-al-numarului-de-ordine

>>> validate('J52/750/2012')
'J52/750/2012'
>>> validate('X52/750/2012')
Traceback (most recent call last):
    ...
InvalidComponent: ...
>>> validate('J2012000750528')
'J2012000750528'
>>> validate('J2012000750529')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
"""  # noqa: E501

from __future__ import annotations

import datetime
import re

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


# These characters should all be replaced by slashes
_cleanup_re = re.compile(r'[ /\\-]+')

# This pattern should match numbers that for some reason have a full date
# as last field for the old format
_old_onrc_fulldate_re = re.compile(r'^([A-Z][0-9]+/[0-9]+/)\d{2}[.]\d{2}[.](\d{4})$')

# This pattern should match all valid numbers in the old format
_old_onrc_re = re.compile(r'^[A-Z][0-9]+/[0-9]+/[0-9]+$')

# List of valid counties
_counties = set(list(range(1, 41)) + [51, 52])


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    number = _cleanup_re.sub('/', clean(number).upper().strip())
    # remove optional slash between first letter and county digits
    if number[1:2] == '/':
        number = number[:1] + number[2:]
    # normalise county number to two digits
    if number[2:3] == '/':
        number = number[:1] + '0' + number[1:]
    # convert trailing full date to year only
    m = _old_onrc_fulldate_re.match(number)
    if m:
        number = ''.join(m.groups())
    return number


def _validate_old_format(number: str) -> None:
    # old YJJ/XXXX/AAAA format
    if not _old_onrc_re.match(number):
        raise InvalidFormat()
    county, serial, year = number[1:].split('/')
    if len(serial) > 5:
        raise InvalidLength()
    if len(county) not in (1, 2) or int(county) not in _counties:
        raise InvalidComponent()
    if len(year) != 4:
        raise InvalidLength()
    # old format numbers will not be issued after 2024
    if int(year) < 1990 or int(year) > 2024:
        raise InvalidComponent()


def _calc_check_digit(number: str) -> str:
    """Calculate the check digit for the new ONRC format."""
    # replace letters with digits
    number = str(ord(number[0]) % 10) + number[1:]
    return str(sum(int(n) for n in number[:-1]) % 10)


def _validate_new_format(number: str) -> None:
    # new YAAAAXXXXXXJJC format, no slashes
    if not isdigits(number[1:]):
        raise InvalidFormat()
    if len(number) != 14:
        raise InvalidLength()
    year = int(number[1:5])
    if year < 1990 or year > datetime.date.today().year:
        raise InvalidComponent()
    # the registration year determines which counties are allowed
    # companies registered after 2024-07-26 have 00 as county code
    county = int(number[11:13])
    if year < 2024:
        if county not in _counties:
            raise InvalidComponent()
    elif year == 2024:
        if county not in _counties.union([0]):
            raise InvalidComponent()
    else:
        if county != 0:
            raise InvalidComponent()
    if number[-1] != _calc_check_digit(number):
        raise InvalidChecksum


def validate(number: str) -> str:
    """Check if the number is a valid ONRC."""
    number = compact(number)
    # J: legal entities (e.g., LLC, SA, etc.)
    # F: sole proprietorships, individual and family businesses
    # C: cooperative societies.
    if number[:1] not in 'JFC':
        raise InvalidComponent()
    if '/' in number:
        # old YJJ/XXXX/AAAA format, still supported but will be phased out, companies
        # will get a new number
        _validate_old_format(number)
    else:
        # new YAAAAXXXXXXJJC format, no slashes
        _validate_new_format(number)
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid ONRC."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
