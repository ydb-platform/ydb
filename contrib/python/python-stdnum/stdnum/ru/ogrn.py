# ogrn.py - functions for handling Russian company registration numbers
# coding: utf-8
#
# Copyright (C) 2024 Ivan Stavropoltsev
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

"""ОГРН, OGRN, PSRN, ОГРНИП, OGRNIP (Russian Primary State Registration Number).

The ОГРН (Основной государственный регистрационный номер, Primary State
Registration Number) is a Russian identifier for legal entities. The number
consists of 13 or 15 digits and includes information on the type of
organisation, the registration year and a tax inspection code. The 15 digit
variant is called ОГРНИП (Основной государственный регистрационный номер
индивидуального предпринимателя, Primary State Registration Number of an
Individual Entrepreneur).

More information:

* https://ru.wikipedia.org/wiki/Основной_государственный_регистрационный_номер
* https://ru.wikipedia.org/wiki/Основной_государственный_регистрационный_номер_индивидуального_предпринимателя

>>> validate('1022200525819')
'1022200525819'
>>> validate('385768585948949')
'385768585948949'
>>> validate('1022500001328')
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
    return clean(number, ' ')


def calc_check_digit(number: str) -> str:
    """Calculate the control digit of the OGRN based on its length."""
    if len(number) == 13:
        return str(int(number[:-1]) % 11 % 10)
    else:
        return str(int(number[:-1]) % 13)


def validate(number: str) -> str:
    """Determine if the given number is a valid OGRN."""
    number = compact(number)
    if not isdigits(number):
        raise InvalidFormat()
    if len(number) == 13:
        if number[0] == '0':
            raise InvalidComponent()
    elif len(number) == 15:
        if number[0] not in '34':
            raise InvalidComponent()
    else:
        raise InvalidLength()
    if number[-1] != calc_check_digit(number):
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid OGRN."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
