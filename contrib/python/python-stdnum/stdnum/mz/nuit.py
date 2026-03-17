# nuit.py - functions for handling Mozambique NUIT numbers
# coding: utf-8
#
# Copyright (C) 2023 Leandro Regueiro
# Copyright (C) 2025 Luca Sicurello
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

"""NUIT (Número Único de Identificação Tributaria, Mozambique tax number).

This number consists of 9 digits, sometimes separated in three groups of three
digits using whitespace to make it easier to read.

The first digit indicates the type of entity. The next seven digits are a
sequential number. The last digit is the check digit, which is used to verify
the number was correctly typed.

More information:

* https://www.mobilize.org.mz/nuit-numero-unico-de-identificacao-tributaria/
* https://www.at.gov.mz/por/Perguntas-Frequentes2/NUIT

>>> validate('400339910')
'400339910'
>>> validate('400 005 834')
'400005834'
>>> validate('12345')
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> format('400339910')
'400 339 910'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation."""
    return clean(number, ' -.').strip()


def calc_check_digit(number: str) -> str:
    """Calculate the check digit."""
    weights = (8, 9, 4, 5, 6, 7, 8, 9)
    check = sum(w * int(n) for w, n in zip(weights, number)) % 11
    return '01234567891'[check]


def validate(number: str) -> str:
    """Check if the number is a valid Mozambique NUIT number."""
    number = compact(number)
    if len(number) != 9:
        raise InvalidLength()
    if not isdigits(number):
        raise InvalidFormat()
    if calc_check_digit(number[:-1]) != number[-1]:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid Mozambique NUIT number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return ' '.join([number[:3], number[3:-3], number[-3:]])
