# coding: utf-8
# ssn.py - function for handling Belgian social security numbers
#
# Copyright (C) 2023-2024 Jeff Horemans
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

"""SSN, INSZ, NISS (Belgian social security number).

The Belgian social security identification number, also known as the INSZ
number (Identificatienummer van de sociale zekerheid), NISS number (Numéro
d'identification de la sécurité sociale) or ENSS number (Erkennungsnummer
der sozialen Sicherheit), is the unique identification number of a person
working in or covered for social benefits in Belgium.

For Belgian residents, the number is identical to their Belgian National
Number (Rijksregisternummer, Numéro National).
For non-residents, the number is identical to their Belgian BIS number.

Both numbers consists of 11 digits and encode a person's date of birth and
gender. It encodes the date of birth in the first 6 digits in the format
YYMMDD. The following 3 digits represent a counter of people born on the same
date, separated by sex (odd for male and even for females respectively). The
final 2 digits form a check number based on the 9 preceding digits.


More information:

* https://www.socialsecurity.be/site_nl/employer/applics/belgianidpro/index.htm
* https://overheid.vlaanderen.be/personeel/regelgeving/insz-nummer
* https://www.ksz-bcss.fgov.be/nl/project/rijksregisterksz-registers

>>> compact('85.07.30-033 28')
'85073003328'
>>> compact('98.47.28-997.65')
'98472899765'
>>> validate('85 07 30 033 28')
'85073003328'
>>> validate('98 47 28 997 65')
'98472899765'
>>> validate('17 07 30 033 84')
'17073003384'
>>> validate('01 49 07 001 85')
'01490700185'
>>> validate('12345678901')
Traceback (most recent call last):
    ...
InvalidChecksum: ...

>>> guess_type('85.07.30-033 28')
'nn'
>>> guess_type('98.47.28-997.65')
'bis'

>>> format('85073003328')
'85.07.30-033.28'
>>> get_birth_date('85.07.30-033 28')
datetime.date(1985, 7, 30)
>>> get_birth_year('85.07.30-033 28')
1985
>>> get_birth_month('85.07.30-033 28')
7
>>> get_gender('85.07.30-033 28')
'M'

>>> format('98472899765')
'98.47.28-997.65'
>>> get_birth_date('98.47.28-997.65')
datetime.date(1998, 7, 28)
>>> get_birth_year('98.47.28-997.65')
1998
>>> get_birth_month('98.47.28-997.65')
7
>>> get_gender('98.47.28-997.65')
'M'

"""

from __future__ import annotations

import datetime

from stdnum.be import bis, nn
from stdnum.exceptions import *


_ssn_modules = (nn, bis)


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return nn.compact(number)


def validate(number: str) -> str:
    """Check if the number is a valid Belgian SSN. This searches for
    the proper sub-type and validates using that."""
    try:
        return bis.validate(number)
    except InvalidComponent:
        # Only try NN validation in case of an invalid component,
        # other validation errors are shared between BIS and NN
        return nn.validate(number)


def is_valid(number: str) -> bool:
    """Check if the number is a valid Belgian SSN number."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def guess_type(number: str) -> str | None:
    """Return the Belgian SSN type for which this number is valid."""
    for mod in _ssn_modules:
        if mod.is_valid(number):
            return mod.__name__.rsplit('.', 1)[-1]
    return None


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    return nn.format(number)


def get_birth_year(number: str) -> int | None:
    """Return the year of the birth date."""
    return nn.get_birth_year(number)


def get_birth_month(number: str) -> int | None:
    """Return the month of the birth date."""
    return nn.get_birth_month(number)


def get_birth_date(number: str) -> datetime.date | None:
    """Return the date of birth."""
    return nn.get_birth_date(number)


def get_gender(number: str) -> str | None:
    """Get the person's gender ('M' or 'F')."""
    for mod in _ssn_modules:
        if mod.is_valid(number):
            return mod.get_gender(number)  # type: ignore[no-any-return]
    return None
