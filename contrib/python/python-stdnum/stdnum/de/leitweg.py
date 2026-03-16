# leitweg.py - functions for handling Leitweg-ID
# coding: utf-8
#
# Copyright (C) 2025 Holvi Payment Services Oy
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

"""Leitweg-ID, a buyer reference or routing identifier for electronic invoices.

For the successful transmission of an electronic invoice as invoicing party or
sender, a unique identification and addressing of the invoice recipient is
required. The Leitweg-ID must be transmitted as a mandatory requirement for
electronic invoicing to public contracting authorities in the federal
administration.

More information:

* https://leitweg-id.de/
* https://de.wikipedia.org/wiki/Leitweg-ID
* https://xeinkauf.de/app/uploads/2022/11/Leitweg-ID-Formatspezifikation-v2-0-2-1.pdf

>>> validate('991-03730-19')
'991-03730-19'
>>> validate('1-03730-19')
Traceback (most recent call last):
    ...
InvalidFormat: ...
"""

from __future__ import annotations

import re

from stdnum.exceptions import *
from stdnum.iso7064 import mod_97_10


_pattern = re.compile(r'[0-9]{2,12}(-[0-9A-Z]{,30})?-[0-9]{2}')


def compact(number: str) -> str:
    """
    Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace.
    """
    return number.strip().upper()  # no valid separators, dashes part of the format


def validate(number: str) -> str:
    """
    Check if the number provided is valid. This checks the format, state or
    federal government code, and check digits.
    """
    if not isinstance(number, str):
        raise InvalidFormat()
    number = compact(number)
    # 2.1 Bestandteile der Leitweg-ID
    if not 5 <= len(number) <= 46:
        raise InvalidLength()
    # 2.1 Bestandteile der Leitweg-ID
    if not re.fullmatch(_pattern, number):
        raise InvalidFormat()
    # 2.2.1 Kennzahl des Bundeslandes/des Bundes
    if not number[:2] in {
        '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
        '11', '12', '13', '14', '15', '16', '99',
    }:
        raise InvalidComponent()
    # 2.4 Prüfziffer
    mod_97_10.validate(number.replace('-', ''))
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is valid. This checks the length and
    check digit."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False
