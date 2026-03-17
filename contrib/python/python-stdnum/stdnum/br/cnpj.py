# cnpj.py - functions for handling CNPJ numbers
# coding: utf-8
#
# Copyright (C) 2015-2026 Arthur de Jong
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

"""CNPJ (Cadastro Nacional da Pessoa Jurídica, Brazilian company identifier).

Numbers from the national register of legal entities have 14 alphanumeric digits.
The first 8 digits identify the company, the following 4 digits identify a
business unit and the last 2 digits are check digits.

>>> validate('16.727.230/0001-97')
'16727230000197'
>>> validate('12. ABC.345 /01DE–35')  # new format from July 2026 onwards
'12ABC34501DE35'
>>> validate('16.727.230.0001-98')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('16.727.230/0001=97')  # invalid delimiter
Traceback (most recent call last):
    ...
InvalidFormat: ...
>>> format('16727230000197')
'16.727.230/0001-97'
"""

from __future__ import annotations

import re

from stdnum.exceptions import *
from stdnum.util import clean


# Minimal regex of valid characters
_cnpj_re = re.compile(r'^[\dA-Z]+$')


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, ' -./').strip().upper()


def calc_check_digits(number: str) -> str:
    """Calculate the check digits for the number."""
    number = compact(number)
    values = [ord(n) - 48 for n in number[:12]]
    weights = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    d1 = (11 - sum(w * v for w, v in zip(weights, values))) % 11 % 10
    values.append(d1)
    weights = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    d2 = (11 - sum(w * v for w, v in zip(weights, values))) % 11 % 10
    return f'{d1}{d2}'


def validate(number: str) -> str:
    """Check if the number is a valid CNPJ. This checks the length and
    whether the check digits are correct."""
    number = compact(number)
    if not _cnpj_re.match(number) or number.startswith('000000000000'):
        raise InvalidFormat()
    if len(number) != 14:
        raise InvalidLength()
    if calc_check_digits(number) != number[-2:]:
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid CNPJ."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return (number[0:2] + '.' + number[2:5] + '.' + number[5:8] + '/' +
            number[8:12] + '-' + number[12:])
