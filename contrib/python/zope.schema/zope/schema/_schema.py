##############################################################################
#
# Copyright (c) 2002 Zope Foundation and Contributors.
# All Rights Reserved.
#
# This software is subject to the provisions of the Zope Public License,
# Version 2.1 (ZPL).  A copy of the ZPL should accompany this distribution.
# THIS SOFTWARE IS PROVIDED "AS IS" AND ANY AND ALL EXPRESS OR IMPLIED
# WARRANTIES ARE DISCLAIMED, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
# WARRANTIES OF TITLE, MERCHANTABILITY, AGAINST INFRINGEMENT, AND FITNESS
# FOR A PARTICULAR PURPOSE.
#
##############################################################################
"""Schema convenience functions
"""

from zope.schema._bootstrapfields import get_schema_validation_errors
from zope.schema._bootstrapfields import get_validation_errors
from zope.schema._bootstrapfields import getFields


__all__ = [
    'getFieldNames',
    'getFields',
    'getFieldsInOrder',
    'getFieldNamesInOrder',
    'getValidationErrors',
    'getSchemaValidationErrors',
]


def getFieldNames(schema):
    """Return a list of all the Field names in a schema.
    """
    return list(getFields(schema).keys())


def getFieldsInOrder(schema, _field_key=lambda x: x[1].order):
    """Return a list of (name, value) tuples in native schema order.
    """
    return sorted(getFields(schema).items(), key=_field_key)


def getFieldNamesInOrder(schema):
    """Return a list of all the Field names in a schema in schema order.
    """
    return [name for name, field in getFieldsInOrder(schema)]


def getValidationErrors(schema, value):
    """
    Validate that *value* conforms to the schema interface *schema*.

    This includes checking for any schema validation errors (using
    `getSchemaValidationErrors`). If that succeeds, then we proceed to
    check for any declared invariants.

    Note that this does not include a check to see if the *value*
    actually provides the given *schema*.

    :return: A sequence of (name, `zope.interface.Invalid`) tuples,
       where *name* is None if the error was from an invariant.
       If the sequence is empty, there were no errors.
    """
    schema_error_dict, invariant_errors = get_validation_errors(
        schema,
        value,
    )

    if not schema_error_dict and not invariant_errors:
        # Valid! Yay!
        return []

    return (
        list(schema_error_dict.items()) +
        [(None, e) for e in invariant_errors]
    )


def getSchemaValidationErrors(schema, value):
    """
    Validate that *value* conforms to the schema interface *schema*.

    All :class:`zope.schema.interfaces.IField` members of the *schema*
    are validated after being bound to *value*. (Note that we do not check for
    arbitrary :class:`zope.interface.Attribute` members being present.)

    :return: A sequence of (name, `ValidationError`) tuples. A non-empty
        sequence indicates validation failed.
    """
    items = get_schema_validation_errors(schema, value).items()
    return items if isinstance(items, list) else list(items)
