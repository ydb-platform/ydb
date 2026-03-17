#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

"""
URN: Uniform Resource Name for DejaCode
=======================================

In the DejaCode platform, a URN is a globally unique and universal key to
reference data across tools and data sources. URNs are used throughout the
platform, such as in ScanCode and related tools as an object key.

URNs are both readable by humans and machines.

The URN syntax is a Internet standard specified in:
 * RFC2141 http://tools.ietf.org/html/rfc2141
 * RFC2396 http://tools.ietf.org/html/rfc2396

A DejaCode URN follows the URN syntax specification and is case sensitive. It
does support UTF-8 characters when these are URL-encoded (using the quote+
encoding).


Syntax and Examples
-------------------

The generic syntax of a DejaCode URN is::
    urn:<namespace>:<object_type>:<fields>
where:
 * <namespace> is always "dje"
 * <object_type> is a DejaCode object type. Not all objects are supported.
   Current support includes owner, license and component.
 * <fields> is one or more object-specific field each field separated by a
   colon.

The syntax for an Owner is::
    urn:dje:owner:<owner_name>
where owner_name is the name of the owner.
Example::
    urn:dje:owner:Apache+Software+Foundation

The syntax for a License is::
    urn:dje:license:<license_key>
where license_key is the key of the license.
Example::
    urn:dje:license:apache-2.0

The syntax for a Component is::
    urn:dje:component:<component_name>:<component_version>
where:
 * component_name is the name of the component
 * component_version is the version of the component. If no version is
 defined, use a trailing colon representing an empty value/undefined version.

Examples:
 * with version::
     urn:dje:component:dropbear:1.0
 * without version::
     urn:dje:component:dropbear:

The product object type syntax is the same as the component syntax.
"""

from urllib.parse import quote_plus
from urllib.parse import unquote_plus


class URNValidationError(Exception):
    """The URN format is not valid."""


# Describes the URN schema for each object type
URN_SCHEMAS = {
    'license': {
        'object': 'license',
        'fields': ['key'],
    },
    'owner': {
        'object': 'owner',
        'fields': ['name'],
    },
    'component': {
        'object': 'component',
        'fields': ['name', 'version'],
    },
    'product': {
        'object': 'product',
        'fields': ['name', 'version'],
    },
}


def encode(object_type, **fields):
    """
    Return an encoded URN based given an object_type string and a dictionary
    of the object-specific fields. All field values must be provided even if
    empty. In this case use an empty string.

    This is a string and local operation only: the URN is NOT resolved and
    therefore validity of the data for the URN as a whole and for each URN
    segment is not checked.
    """

    # case is not significant for the object type
    object_type = object_type.strip().lower()
    urn_prefix = 'urn:dje:{0}:'.format(quote_plus(object_type))

    object_fields = URN_SCHEMAS[object_type]['fields']
    # leading and trailing white spaces are not significant
    # each URN part is encoded individually BEFORE assembling the URN
    encoded_fields = [quote_plus(fields[f].strip()) for f in object_fields]
    encoded_fields = ':'.join(encoded_fields)
    return urn_prefix + encoded_fields


def decode(urn):
    """
    Decode a URN and return the object_type and a mapping of field/values.
    Raise URNValidationError on errors.
    """
    segments = [unquote_plus(p) for p in urn.split(':')]

    if not segments[0] == ('urn'):
        raise URNValidationError("Invalid URN prefix. Expected 'urn'.")

    if not segments[1] == ('dje'):
        raise URNValidationError("Invalid URN namespace. Expected 'dje'.")

    # object type is always lowercase
    object_type = segments[2].lower()
    if object_type not in URN_SCHEMAS:
        raise URNValidationError('Unsupported URN object type.')

    fields = segments[3:]
    schema_fields = URN_SCHEMAS[object_type]['fields']
    if len(schema_fields) != len(fields):
        raise URNValidationError('Invalid number of fields in URN.')
    decoded_fields = dict(zip(schema_fields, fields))

    return object_type, decoded_fields
