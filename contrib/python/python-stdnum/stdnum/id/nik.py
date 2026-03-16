# nik.py - functions for handling Indonesian NIK numbers
# coding: utf-8
#
# Copyright (C) 2024 Arthur de Jong
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

"""NIK (Nomor Induk Kependudukan, Indonesian identity number).


The Nomor Induk Kependudukan (NIK, Population Identification Number,
sometimes known as Nomor Kartu Tanda Penduduk or Nomor KTP) is issued to
Indonesian citizens.

The number consists of 16 digits in the format PPRRSSDDMMYYXXXX where PPRRSS
(province, city/district, sub-district) indicates the place of residence when
the number was issued. It is followed by a DDMMYY date of birth (for female
40 is added to the day). The last 4 digits are used to make the number
unique.

More information:

* https://id.wikipedia.org/wiki/Nomor_Induk_Kependudukan

>>> validate('3171011708450001')
'3171011708450001'
>>> validate('31710117084500')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> validate('9971011708450001')  # invalid province
Traceback (most recent call last):
    ...
InvalidComponent: ...
>>> get_birth_date('3171015708450001')
datetime.date(1945, 8, 17)
>>> get_birth_date('3171012902001234')  # 1900-02-29 doesn't exist
datetime.date(2000, 2, 29)
>>> get_birth_date('3171013002001234')  # 1900-20-30 doesn't exist
Traceback (most recent call last):
    ...
InvalidComponent: ...
"""

from __future__ import annotations

import datetime

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation.

    This strips the number of any valid separators and removes
    surrounding whitespace.
    """
    return clean(number, ' -.').strip()


def get_birth_date(number: str, minyear: int = 1920) -> datetime.date:
    """Get the birth date from the person's NIK.

    Note that the number only encodes the last two digits of the year so
    this may be a century off.
    """
    number = compact(number)
    day = int(number[6:8]) % 40
    month = int(number[8:10])
    year = int(number[10:12])
    try:
        return datetime.date(year + 1900, month, day)
    except ValueError:
        pass
    try:
        return datetime.date(year + 2000, month, day)
    except ValueError:
        raise InvalidComponent()


def _check_registration_place(number: str) -> dict[str, str]:
    """Use the number to look up the place of registration of the person."""
    from stdnum import numdb
    results = numdb.get('id/loc').info(number[:4])[0][1]
    if not results:
        raise InvalidComponent()
    return results


def validate(number: str) -> str:
    """Check if the number is a valid Indonesian NIK."""
    number = compact(number)
    if not isdigits(number):
        raise InvalidFormat()
    if len(number) != 16:
        raise InvalidLength()
    get_birth_date(number)
    _check_registration_place(number)
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Indonesian NIK."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
