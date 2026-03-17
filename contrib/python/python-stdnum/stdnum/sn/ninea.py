# ninea.py - functions for handling Senegal NINEA numbers
# coding: utf-8
#
# Copyright (C) 2023 Leandro Regueiro
# Copyright (C) 2026 Arthur de Jong
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

"""NINEA (Numéro d'Identification Nationale des Entreprises et Associations, Senegal tax number).

The National Identification Number for Businesses and Associations (NINEA) is
a unique tax identifier for tax purposes in Senegal.

This number consists of 7 digits and is usually followed by a 3 digit tax
identification code called COFI (Code d’Identification Fiscale) that is used
to indicate the company's tax status and legal structure.

More information:

* https://www.wikiprocedure.com/index.php/Senegal%5F-_Obtain_a_Tax_Identification_Number
* https://nkac-audit.com/comprendre-le-ninea-votre-guide-de-lecture-simplifie/
* https://www.creationdentreprise.sn/rechercher-une-societe
* https://www.dci-sn.sn/index.php/obtenir-son-ninea
* https://e-ninea.ansd.sn/search_annuaire

>>> validate('306 7221')
'3067221'
>>> validate('30672212G2')
'30672212G2'
>>> validate('3067221 2G2')
'30672212G2'
>>> validate('3067222')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> validate('1234567 0AZ')
Traceback (most recent call last):
    ...
InvalidComponent: ...
>>> format('30672212G2')
'3067221 2G2'
"""  # noqa: E501

from stdnum.exceptions import *
from stdnum.util import clean, isdigits


def compact(number: str) -> str:
    """Convert the number to the minimal representation.

    This strips the number of any valid separators and removes surrounding
    whitespace.
    """
    return clean(number, ' -/,').upper().strip()


def _validate_cofi(number: str) -> None:
    # The first digit of the COFI indicates the tax status
    # 0: taxpayer subject to the real scheme, not subject to VAT.
    # 1: taxpayer subject to the single global contribution (TOU).
    # 2: taxpayer subject to the real scheme and subject to VAT.
    if number[0] not in '012':
        raise InvalidComponent()
    # The second character is a letter that indicates the tax centre:
    # A: Dakar Plateau 1
    # B: Dakar Plateau 2
    # C: Grand Dakar
    # D: Pikine
    # E: Rufisque
    # F: Thiès
    # G: Centre des grandes Entreprises
    # H: Louga
    # J: Diourbel
    # K: Saint-Louis
    # L: Tambacounda
    # M: Kaolack
    # N: Fatick
    # P: Ziguinchor
    # Q: Kolda
    # R: Prarcelles assainies
    # S: Professions libérales
    # T: Guédiawaye
    # U: Dakar-Medina
    # V: Dakar liberté
    # W: Matam
    # Z: Centre des Moyennes Entreprises
    if number[1] not in 'ABCDEFGHJKLMNPQRSTUVWZ':
        raise InvalidComponent()
    # The third character is a digit that indicates the legal form:
    # 1: Individual-Natural person
    # 2: SARL
    # 3: SA
    # 4: Simple Limited Partnership
    # 5: Share Sponsorship Company
    # 6: GIE
    # 7: Civil Society
    # 8: Partnership
    # 9: Cooperative Association
    # 0: Other
    if number[2] not in '0123456789':
        raise InvalidComponent()


def _checksum(number: str) -> int:
    number = number.zfill(9)
    weights = (1, 2, 1, 2, 1, 2, 1, 2, 1)
    return sum(int(n) * w for n, w in zip(number, weights)) % 10


def validate(number: str) -> str:
    """Check if the number is a valid Senegal NINEA.

    This checks the length and formatting.
    """
    cofi = ''
    number = compact(number)
    if len(number) > 9:
        cofi = number[-3:]
        number = number[:-3]
    if len(number) not in (7, 9):
        raise InvalidLength()
    if not isdigits(number):
        raise InvalidFormat()
    if cofi:
        _validate_cofi(cofi)
    if _checksum(number) != 0:
        raise InvalidChecksum()
    return number + cofi


def is_valid(number: str) -> bool:
    """Check if the number is a valid Senegal NINEA."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    if len(number) in (7, 9):
        return number
    return ' '.join([number[:-3], number[-3:]])
