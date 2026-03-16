# nace.py - functions for handling EU NACE classification
# coding: utf-8
#
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

"""NACE (classification for businesses in the European Union).

The NACE (nomenclature statistique des activités économiques dans la
Communauté européenne) is a 4-level (and up to 4 digit) code for classifying
economic activities. It is the European implementation of the UN
classification ISIC.

The first 4 digits are the same in all EU countries while additional levels
and digits may be vary between countries. This module validates the numbers
according to revision 2.1 (or 2.0 if that version is supplied) and based on
the registry as published by the EC.

More information:

* https://en.wikipedia.org/wiki/Statistical_Classification_of_Economic_Activities_in_the_European_Community
* https://ec.europa.eu/eurostat/ramon/nomenclatures/index.cfm?TargetUrl=LST_NOM_DTL&StrNom=NACE_REV2&StrLanguageCode=EN&IntPcKey=&StrLayoutCode=HIERARCHIC
* https://showvoc.op.europa.eu/#/datasets/ESTAT_Statistical_Classification_of_Economic_Activities_in_the_European_Community_Rev._2.1._%28NACE_2.1%29/data

>>> validate('A')
'A'
>>> validate('62.01', revision='2.0')
'6201'
>>> get_label('62.10')
'Computer programming activities'
>>> validate('62.05')
Traceback (most recent call last):
    ...
InvalidComponent: ...
>>> validate('62059')  # does not validate country-specific numbers
Traceback (most recent call last):
    ...
InvalidLength: ...
>>> format('6201')
'62.01'
"""  # noqa: E501

from __future__ import annotations

import warnings

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


# The revision of the NACE definition to use
DEFAULT_REVISION = '2.1'


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips the
    number of any valid separators and removes surrounding whitespace."""
    return clean(number, '.').strip()


def info(number: str, *, revision: str = DEFAULT_REVISION) -> dict[str, str]:
    """Lookup information about the specified NACE. This returns a dict."""
    number = compact(number)
    revision = revision.replace('.', '')
    from stdnum import numdb
    info = dict()
    for _n, i in numdb.get(f'eu/nace{revision}').info(number):
        if not i:
            raise InvalidComponent()
        info.update(i)
    return info


def get_label(number: str, revision: str = DEFAULT_REVISION) -> str:
    """Lookup the category label for the number."""
    return info(number, revision=revision)['label']


def label(number: str) -> str:  # pragma: no cover (deprecated function)
    """DEPRECATED: use `get_label()` instead."""  # noqa: D40
    warnings.warn(
        'label() has been to get_label()',
        DeprecationWarning, stacklevel=2)
    return get_label(number, revision='2.0')


def validate(number: str, revision: str = DEFAULT_REVISION) -> str:
    """Check if the number is a valid NACE. This checks the format and
    searches the registry to see if it exists."""
    number = compact(number)
    if len(number) > 4:
        raise InvalidLength()
    elif len(number) == 1:
        if not number.isalpha():
            raise InvalidFormat()
    else:
        if not isdigits(number):
            raise InvalidFormat()
    info(number, revision=revision)
    return number


def is_valid(number: str, revision: str = DEFAULT_REVISION) -> bool:
    """Check if the number is a valid NACE. This checks the format and
    searches the registry to see if it exists."""
    try:
        return bool(validate(number, revision=revision))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    return '.'.join((number[:2], number[2:])).strip('.')
