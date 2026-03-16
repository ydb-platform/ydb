#
# Copyright (C) 2015 - 2026 Satoru SATOH <satoru.satoh gmail.com>
# SPDX-License-Identifier: MIT
#
"""JSON schema generator."""
from __future__ import annotations

import typing

from ... import utils

if typing.TYPE_CHECKING:
    import collections.abc
    from ..datatypes import (
        InDataExT, InDataT,
    )


_TYPE_MAP: dict[type, str] = {
    bool: "boolean",
    dict: "object",
    float: "number",
    int: "integer",
    list: "array",
    str: "string",
    tuple: "array",
}


def _process_options(
    **options: typing.Any,
) -> tuple[dict[typing.Any, typing.Any], bool]:
    """Help to process keyword arguments passed to gen_schema.

    :return: A tuple of (typemap :: dict, strict :: bool)
    """
    return (
        options.get("ac_schema_typemap", _TYPE_MAP),
        bool(options.get("ac_schema_strict", False)),
    )


def array_to_schema(
    iarr: collections.abc.Iterable[InDataExT], *,
    ac_schema_typemap: dict[type, str] | None = None,
    ac_schema_strict: bool = False,
    **options: typing.Any,
) -> InDataT:
    """Generate a JSON schema object with type annotation added for ``iaa```.

    :param arr: Array of mapping objects like dicts
    :param options: Other keyword options such as:

        - ac_schema_strict: True if more strict (precise) schema is needed
        - ac_schema_typemap: Type to JSON schema type mappings

    :return: Another mapping objects represents JSON schema of items
    """
    arr: list[InDataExT] = list(iarr)
    typemap = ac_schema_typemap or _TYPE_MAP
    scm: dict[str, typing.Any] = {
        "type": typemap[list],
        "items": gen_schema(
            arr[0] if arr else "str",
            ac_schema_strict=ac_schema_strict,
            **options,
        ),
    }
    if ac_schema_strict:
        nitems = len(arr)
        scm["minItems"] = nitems
        scm["uniqueItems"] = len(set(arr)) == nitems

    return scm


def object_to_schema(
    obj: InDataT, *,
    ac_schema_typemap: dict[type, str] | None = None,
    ac_schema_strict: bool = False,
    **options: typing.Any,
) -> InDataT:
    """Generate a node represents JSON schema object for ``obj``.

    Type annotation will be added for given object node at the same time.

    :param obj: mapping object such like a dict
    :param options: Other keyword options such as:

        - ac_schema_strict: True if more strict (precise) schema is needed
        - ac_schema_typemap: Type to JSON schema type mappings

    :yield: Another mapping objects represents JSON schema of object
    """
    typemap = ac_schema_typemap or _TYPE_MAP

    props = {
        k: gen_schema(
            v,
            ac_schema_typemap=ac_schema_typemap,
            ac_schema_strict=ac_schema_strict,
            **options,
        )
        for k, v in obj.items()
    }
    scm = {"type": typemap[dict], "properties": props}
    if ac_schema_strict:
        scm["required"] = sorted(props.keys())

    return scm


def gen_schema(
    data: InDataExT, **options: typing.Any,
) -> InDataT:
    """Generate a JSON schema object validates ``data``.

    :param data: Configuration data object (dict[-like] or namedtuple)
    :param options: Other keyword options such as:

        - ac_schema_strict: True if more strict (precise) schema is needed
        - ac_schema_typemap: Type to JSON schema type mappings

    :return: A dict represents JSON schema of this node
    """
    if data is None:
        return {"type": "null"}

    typemap = options.get("ac_schema_typemap", False) or _TYPE_MAP

    if utils.is_primitive_type(data):
        scm = {"type": typemap[type(data)]}

    elif utils.is_dict_like(data):
        scm = object_to_schema(data, **options)

    elif utils.is_list_like(data):
        scm = array_to_schema(data, **options)

    return scm
