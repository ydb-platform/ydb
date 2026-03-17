# uid.py - functions for handling Swiss business identifiers
# coding: utf-8
#
# Copyright (C) 2015-2024 Arthur de Jong
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

"""UID (Unternehmens-Identifikationsnummer, Swiss business identifier).

The Swiss UID is used to uniquely identify businesses for taxation purposes.
The number consists of a fixed "CHE" prefix, followed by 9 digits that are
protected with a simple checksum.

This module only supports the "new" format that was introduced in 2011 which
completely replaced the "old" 6-digit format in 2014.
Stripped numbers without the CHE prefix are allowed and validated,
but are returned with the prefix prepended.

More information:

* https://www.uid.admin.ch/
* https://de.wikipedia.org/wiki/Unternehmens-Identifikationsnummer

>>> validate('CHE-100.155.212')
'CHE100155212'
>>> validate('100.155.212')
'CHE100155212'
>>> validate('CHE-100.155.213')
Traceback (most recent call last):
    ...
InvalidChecksum: ...
>>> format('CHE100155212')
'CHE-100.155.212'
"""

from __future__ import annotations

from stdnum.exceptions import *
from stdnum.util import clean, get_soap_client, isdigits


TYPE_CHECKING = False
if TYPE_CHECKING:  # pragma: no cover (typechecking only import)
    from typing import Any


def compact(number: str) -> str:
    """Convert the number to the minimal representation. This strips
    surrounding whitespace and separators."""
    number = clean(number, ' -.').strip().upper()
    if len(number) == 9 and isdigits(number):
        number = 'CHE' + number
    return number


def calc_check_digit(number: str) -> str:
    """Calculate the check digit for organisations. The number passed should
    not have the check digit included."""
    weights = (5, 4, 3, 2, 7, 6, 5, 4)
    s = sum(w * int(n) for w, n in zip(weights, number))
    return str((11 - s) % 11)


def validate(number: str) -> str:
    """Check if the number is a valid UID. This checks the length, formatting
    and check digit."""
    number = compact(number)
    if len(number) != 12:
        raise InvalidLength()
    if not number.startswith('CHE'):
        raise InvalidComponent()
    if not isdigits(number[3:]):
        raise InvalidFormat()
    if number[-1] != calc_check_digit(number[3:-1]):
        raise InvalidChecksum()
    return number


def is_valid(number: str) -> bool:
    """Check if the number is a valid UID."""
    try:
        return bool(validate(number))
    except ValidationError:
        return False


def format(number: str) -> str:
    """Reformat the number to the standard presentation format."""
    number = compact(number)
    return number[:3] + '-' + '.'.join(
        number[i:i + 3] for i in range(3, len(number), 3))


uid_wsdl = 'https://www.uid-wse.admin.ch/V5.0/PublicServices.svc?wsdl'


def check_uid(
    number: str,
    timeout: float = 30,
    verify: bool | str = True,
) -> dict[str, Any] | None:  # pragma: no cover
    """Look up information via the Swiss Federal Statistical Office web service.

    This uses the UID registry web service run by the the Swiss Federal
    Statistical Office to provide more details on the provided number.

    The `timeout` argument specifies the network timeout in seconds.

    The `verify` argument is either a boolean that determines whether the
    server's certificate is validate or a string which must be a path the CA
    certificate bundle to use for verification.

    Returns a dict-like object for valid numbers with the following structure::

        {
            'organisation': {
                'organisationIdentification': {
                    'uid': {'uidOrganisationIdCategorie': 'CHE', 'uidOrganisationId': 113690319},
                    'OtherOrganisationId': [
                        {'organisationIdCategory': 'CH.ESTVID', 'organisationId': '052.0111.1006'},
                    ],
                    'organisationName': 'Staatssekretariat für Migration SEM Vermietung von Parkplätzen',
                    'legalForm': '0220',
                },
                'address': [
                    {
                        'addressCategory': 'LEGAL',
                        'street': 'Quellenweg',
                        'houseNumber': '6',
                        'town': 'Wabern',
                        'countryIdISO2': 'CH',
                    },
                ],
            },
            'uidregInformation': {
                'uidregStatusEnterpriseDetail': '3',
                ...
            },
            'vatRegisterInformation': {
                'vatStatus': '2',
                'vatEntryStatus': '1',
                ...
            },
        }

    See the following document for more details on the GetByUID return value
    https://www.bfs.admin.ch/bfs/en/home/registers/enterprise-register/enterprise-identification/uid-register/uid-interfaces.html
    """
    # this function isn't always tested because it would require network access
    # for the tests and might unnecessarily load the web service
    number = compact(number)
    client = get_soap_client(uid_wsdl, timeout=timeout, verify=verify)
    try:
        return client.GetByUID(  # type: ignore[no-any-return]
            uid={'uidOrganisationIdCategorie': number[:3], 'uidOrganisationId': number[3:]},
        )[0]
    except Exception:  # noqa: B902 (exception type depends on SOAP client)
        # Error responses by the server seem to result in exceptions raised
        # by the SOAP client implementation
        return None
