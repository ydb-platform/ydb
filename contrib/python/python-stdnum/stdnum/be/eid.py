# eid.py - functions for handling Belgian Identity Card Number.
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

"""eID Number (Belgian electronic Identity Card Number).

The eID is an electronic identity card (with chip), issued to Belgian citizens
over 12 years old.
The card number applies only to the card in question and should not be confused
with the Belgian National Number (Rijksregisternummer, Numéro National),
that is also included on the card.

The card number consists of 12 digits in the form xxx-xxxxxxx-yy where
yy is a check digit calculated as the remainder of dividing xxxxxxxxxx by 97.
If the remainder is 0, the check number is set to 97.

More information:

* https://www.ibz.rrn.fgov.be/nl/identiteitsdocumenten/eid/
* https://eid.belgium.be/en/what-eid
* https://www.ibz.rrn.fgov.be/fileadmin/user_upload/nl/rr/instructies/IT-lijst/IT195_Identiteitsbewijs_20240115.pdf

>>> compact('000-0011032-71')
'000001103271'
>>> compact('591-1917064-58')
'591191706458'
>>> validate('000-0011032-71')
'000001103271'
>>> validate('591-1917064-58')
'591191706458'
>>> validate('591-2010999-97')
'591201099997'
>>> validate('000-0011032-25')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> format('591191706458')
'591-1917064-58'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, ' -./').upper().strip()


def _calc_check_digits(number: str) -> str:
    """Calculate the expected check digits for the number, calculated as
    the remainder of dividing the first 10 digits of the number by 97.
    If the remainder is 0, the check number is set to 97.
    """
    return '%02d' % ((int(number[:10]) % 97) or 97)


def validate(number: str) -> str:
    """Check if the number is a valid ID card number.
    This checks the length, formatting and check digit."""
    number = compact(number)
    if not isdigits(number) or int(number) <= 0:
        raise InvalidFormat()
    if len(number) != 12:
        raise InvalidLength()
    if _calc_check_digits(number[:-2]) != number[-2:]:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Belgian ID Card number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return '-'.join((number[:3], number[3:10], number[10:]))
