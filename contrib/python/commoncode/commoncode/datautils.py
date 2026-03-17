#
# Copyright (c) nexB Inc. and others. All rights reserved.
# SPDX-License-Identifier: Apache-2.0
# See http://www.apache.org/licenses/LICENSE-2.0 for the license text.
# See https://github.com/nexB/commoncode for support or download.
# See https://aboutcode.org for more information about nexB OSS projects.
#

import attr
from attr.validators import in_ as choices  # NOQA
import typing

"""
Utilities and helpers for data classes.
"""

HELP_METADATA = '__field_help'
LABEL_METADATA = '__field_label'


def attribute(default=attr.NOTHING, validator=None,
              repr=False, eq=True, order=True,  # NOQA
              init=True, type=None, converter=None,  # NOQA
              help=None, label=None, metadata=None,):  # NOQA
    """
    A generic attribute with help metadata and that is not included in the
    representation by default.
    """
    metadata = metadata or dict()
    if help:
        metadata[HELP_METADATA] = help

    if label:
        metadata[LABEL_METADATA] = label

    return attr.attrib(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=init,
        metadata=metadata,
        type=type,
        converter=converter
    )


def Boolean(default=False, validator=None, repr=False, eq=True, order=True,  # NOQA
            converter=None, label=None, help=None,):  # NOQA
    """
    A boolean attribute.
    """
    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=bool,
        converter=converter,
        help=help,
        label=label,
    )


def TriBoolean(default=None, validator=None, repr=False, eq=True, order=True,  # NOQA
            converter=None, label=None, help=None,):  # NOQA
    """
    A tri-boolean attribute with possible values of None, True and False.
    """
    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=bool,
        converter=converter,
        help=help,
        label=label,
    )


def String(default=None, validator=None, repr=False, eq=True, order=True,  # NOQA
           converter=None, label=None, help=None,):  # NOQA
    """
    A string attribute.
    """
    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=str,
        converter=converter,
        help=help,
        label=label,
    )


def Integer(default=0, validator=None, repr=False, eq=True, order=True,  # NOQA
            converter=None, label=None, help=None,):  # NOQA
    """
    An integer attribute.
    """
    converter = converter or attr.converters.optional(int)
    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=int,
        converter=converter,
        help=help,
        label=label,
    )


def Float(default=0.0, validator=None, repr=False, eq=True, order=True,  # NOQA
          converter=None, label=None, help=None,):  # NOQA
    """
    A float attribute.
    """
    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=float,
        converter=converter,
        help=help,
        label=label,
    )


def List(item_type=typing.Any, default=attr.NOTHING, validator=None,
         repr=False, eq=True, order=True,  # NOQA
         converter=None, label=None, help=None,):  # NOQA
    """
    A list attribute: the optional item_type defines the type of items it stores.
    """
    if default is attr.NOTHING:
        default = attr.Factory(list)

    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=typing.List[item_type],
        converter=converter,
        help=help,
        label=label,
    )


def Mapping(value_type=typing.Any, default=attr.NOTHING, validator=None,
            repr=False, eq=True, order=True,  # NOQA
            converter=None, help=None, label=None):  # NOQA
    """
    A mapping attribute: the optional value_type defines the type of values it
    stores. The key is always a string.

    Notes: in Python 2 the type is Dict as there is no typing available for
    dict for now.
    """
    if default is attr.NOTHING:
        default = attr.Factory(dict)

    return attribute(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        init=True,
        type=typing.Dict[str, value_type],
        converter=converter,
        help=help,
        label=label,
    )

##################################################
# FIXME: add proper support for dates!!!
##################################################


def Date(default=None, validator=None, repr=False, eq=True, order=True,  # NOQA
           converter=None, label=None, help=None,):  # NOQA
    """
    A date attribute. It always serializes to an ISO date string.
    Behavior is TBD and for now this is exactly a string.
    """
    return String(
        default=default,
        validator=validator,
        repr=repr,
        eq=eq,
        order=order,
        converter=converter,
        help=help,
        label=label,
    )
