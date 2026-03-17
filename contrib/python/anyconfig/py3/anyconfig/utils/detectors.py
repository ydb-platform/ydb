#
# Copyright (C) 2012 - 2024 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""Functions to detect something."""
from __future__ import annotations

import collections.abc
import types
import typing

if typing.TYPE_CHECKING:
    try:
        from typing import TypeGuard
    except ImportError:
        from typing_extensions import TypeGuard


PRIMITIVE_TYPES = (bool, int, float, str, bytes)
PrimitiveType = typing.Union[bool, int, float, str, bytes]


def is_primitive_type(obj: typing.Any) -> TypeGuard[PrimitiveType]:
    """Test if given object is a primitive type."""
    return type(obj) in PRIMITIVE_TYPES


def is_iterable(obj: typing.Any) -> TypeGuard[collections.abc.Iterable]:
    """Test if given object is an iterable object."""
    return (isinstance(obj, (list, tuple, types.GeneratorType))
            or (not isinstance(obj, (int, str, dict))
                and bool(getattr(obj, "next", False))))


def is_dict_like(obj: typing.Any) -> TypeGuard[dict]:
    """Test if given object ``obj`` is an dict."""
    return isinstance(obj, (dict, collections.abc.Mapping))  # any others?


_LIST_LIKE_TYPES = (collections.abc.Iterable, collections.abc.Sequence)


def is_list_like(obj: typing.Any) -> TypeGuard[collections.abc.Iterable]:
    """Test if given object ``obj`` is a list or -like one."""
    return (
        isinstance(obj, _LIST_LIKE_TYPES)
        and not (isinstance(obj, str) or is_dict_like(obj))
    )
