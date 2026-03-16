# voen.py - functions for handling Azerbaijan VOEN numbers
# coding: utf-8
#
# Copyright (C) 2022 Leandro Regueiro
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

"""VÖEN (Vergi ödəyicisinin eyniləşdirmə nömrəsi, Azerbaijan tax number).

The Vergi ödəyicisinin eyniləşdirmə nömrəsi is issued by the Azerbaijan state
tax authorities to individuals and legal entities.

This number consists of 10 digits. The first two digits are the code for the
territorial administrative unit. The following six digits are a serial
number. The ninth digit is a check digit. The tenth digit represents the
legal status of a taxpayer: 1 for legal persons and 2 for natural persons.

More information:

* https://www.oecd.org/tax/automatic-exchange/crs-implementation-and-assistance/tax-identification-numbers/Azerbaijan-TIN.pdf
* https://www.e-taxes.gov.az/ebyn/payerOrVoenChecker.jsp

>>> validate('140 155 5071')
'1401555071'
>>> validate('140 155 5081')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('1400057424')
Traceback (most recent call last):
    ...
InvalidComponent: ...
"""  # noqa: E501

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation.

    This strips the number of any valid separators and removes surrounding
    whitespace.
    """
    number = clean(number, ' ')
    if len(number) == 9:
        number = '0' + number
    return number


def _calc_check_digit(number: str) -> str:
    """Calculate the check digit for the VÖEN."""
    weights = [4, 1, 8, 6, 2, 7, 5, 3]
    return str(sum(w * int(n) for w, n in zip(weights, number)) % 11)


def validate(number: str) -> str:
    """Check if the number is a valid Azerbaijan VÖEN number."""
    number = compact(number)
    if len(number) != 10:
        raise InvalidLength()
    if not isdigits(number):
        raise InvalidFormat()
    if number[-1] not in ('1', '2'):
        raise InvalidComponent()
    if number[-2:-1] != _calc_check_digit(number):
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Azerbaijan VÖEN number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
