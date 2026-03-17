# bic.py - functions for handling ISO 9362 Business identifier codes
#
# Copyright (C) 2015 Lifealike Ltd
# Copyright (C) 2017-2025 Arthur de Jong
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

"""BIC (ISO 9362 Business identifier codes).

An ISO 9362 identifier (also: BIC, BEI, or SWIFT code) uniquely
identifies an institution. They are commonly used to route financial
transactions.

The code consists of a 4 letter institution code, a 2 letter country code,
and a 2 character location code, optionally followed by a three character
branch code.

More information:

* https://en.wikipedia.org/wiki/ISO_9362

>>> validate('AGRIFRPP882')
'AGRIFRPP882'
>>> validate('ABNA BE 2A')
'ABNABE2A'
>>> validate('AGRIFRPP')
'AGRIFRPP'
>>> validate('AGRIFRPP8')
Traceback (most recent call last):
    ...
InvalidLength: ..
>>> validate('AGRIF2PP')  # country code can't contain digits
Traceback (most recent call last):
    ...
InvalidFormat: ..
>>> format('agriFRPP')  # conventionally caps
'AGRIFRPP'

"""

from __future__ import annotations

import re

from stdnum.exceptions import *
from stdnum.util import clean


_bic_re = re.compile(r'^[A-Z]{4}(?P<country_code>[A-Z]{2})[0-9A-Z]{2}([0-9A-Z]{3})?$')


# Valid BIC country codes are ISO 3166-1 alpha-2 with the addition of
# XK for the Republic of Kosovo
# It seems that some of the countries included here don't currently have
# any banks with a BIC code (e.g. Antarctica, Iran, Myanmar)
_country_codes = {
    'AD', 'AE', 'AF', 'AG', 'AI', 'AL', 'AM', 'AO', 'AQ', 'AR', 'AS', 'AT', 'AU',
    'AW', 'AX', 'AZ', 'BA', 'BB', 'BD', 'BE', 'BF', 'BG', 'BH', 'BI', 'BJ', 'BL',
    'BM', 'BN', 'BO', 'BQ', 'BR', 'BS', 'BT', 'BV', 'BW', 'BY', 'BZ', 'CA', 'CC',
    'CD', 'CF', 'CG', 'CH', 'CI', 'CK', 'CL', 'CM', 'CN', 'CO', 'CR', 'CU', 'CV',
    'CW', 'CX', 'CY', 'CZ', 'DE', 'DJ', 'DK', 'DM', 'DO', 'DZ', 'EC', 'EE', 'EG',
    'EH', 'ER', 'ES', 'ET', 'FI', 'FJ', 'FK', 'FM', 'FO', 'FR', 'GA', 'GB', 'GD',
    'GE', 'GF', 'GG', 'GH', 'GI', 'GL', 'GM', 'GN', 'GP', 'GQ', 'GR', 'GS', 'GT',
    'GU', 'GW', 'GY', 'HK', 'HM', 'HN', 'HR', 'HT', 'HU', 'ID', 'IE', 'IL', 'IM',
    'IN', 'IO', 'IQ', 'IR', 'IS', 'IT', 'JE', 'JM', 'JO', 'JP', 'KE', 'KG', 'KH',
    'KI', 'KM', 'KN', 'KP', 'KR', 'KW', 'KY', 'KZ', 'LA', 'LB', 'LC', 'LI', 'LK',
    'LR', 'LS', 'LT', 'LU', 'LV', 'LY', 'MA', 'MC', 'MD', 'ME', 'MF', 'MG', 'MH',
    'MK', 'ML', 'MM', 'MN', 'MO', 'MP', 'MQ', 'MR', 'MS', 'MT', 'MU', 'MV', 'MW',
    'MX', 'MY', 'MZ', 'NA', 'NC', 'NE', 'NF', 'NG', 'NI', 'NL', 'NO', 'NP', 'NR',
    'NU', 'NZ', 'OM', 'PA', 'PE', 'PF', 'PG', 'PH', 'PK', 'PL', 'PM', 'PN', 'PR',
    'PS', 'PT', 'PW', 'PY', 'QA', 'RE', 'RO', 'RS', 'RU', 'RW', 'SA', 'SB', 'SC',
    'SD', 'SE', 'SG', 'SH', 'SI', 'SJ', 'SK', 'SL', 'SM', 'SN', 'SO', 'SR', 'SS',
    'ST', 'SV', 'SX', 'SY', 'SZ', 'TC', 'TD', 'TF', 'TG', 'TH', 'TJ', 'TK', 'TL',
    'TM', 'TN', 'TO', 'TR', 'TT', 'TV', 'TW', 'TZ', 'UA', 'UG', 'UM', 'US', 'UY',
    'UZ', 'VA', 'VC', 'VE', 'VG', 'VI', 'VN', 'VU', 'WF', 'WS', 'XK', 'YE', 'YT',
    'ZA', 'ZM', 'ZW',
}


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any surrounding whitespace."""
    return clean(number, ' -').strip().upper()


def validate(number: str) -> str:
    """Check if the number is a valid routing number. This checks the length
    and characters in each position."""
    number = compact(number)
    if len(number) not in (8, 11):
        raise InvalidLength()
    match = _bic_re.search(number)
    if not match:
        raise InvalidFormat()
    if match.group('country_code') not in _country_codes:
        raise InvalidComponent()
    return number


def is_valid(number: str) -> bool:
    """Check if the number provided is a valid BIC."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    return compact(number)
