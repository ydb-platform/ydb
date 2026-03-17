"""The JSON Schema generator for Logfire.

There are custom keywords in the generated JSON Schema. They are prefixed with `x-` to avoid
conflicts with the official keywords. The custom keywords are:

- `x-python-datatype`: The Python data type of the value. It is used to generate the Python type hints.
- `x-columns`: The column names of the data frame. It is used to generate the Python type.
- `x-indices`: The index names of the data frame. It is used to generate the Python type.
- `x-column-count`: The number of columns in the data frame. It is used to generate the Python type.
- `x-row-count`: The number of rows in the data frame. It is used to generate the Python type.
- `x-shape`: The shape of the numpy array. It is used to generate the Python type.
- `x-dtype`: The data type of the numpy array. It is used to generate the Python type.
"""

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import re
import uuid
from collections import deque
from collections.abc import Iterable, Mapping, Sequence
from contextlib import suppress
from decimal import Decimal
from enum import Enum
from functools import lru_cache
from ipaddress import IPv4Address, IPv4Interface, IPv4Network, IPv6Address, IPv6Interface, IPv6Network
from pathlib import PosixPath
from types import GeneratorType
from typing import Any, Callable, NewType, cast

from .constants import ATTRIBUTES_SCRUBBED_KEY
from .json_encoder import is_attrs, is_sqlalchemy, to_json_value
from .stack_info import STACK_INFO_KEYS
from .utils import JsonDict, dump_json, log_internal_error, safe_repr

__all__ = 'create_json_schema', 'attributes_json_schema_properties', 'attributes_json_schema', 'JsonSchemaProperties'


@lru_cache
def type_to_schema() -> dict[type[Any], JsonDict | Callable[[Any, set[int]], JsonDict]]:
    lookup: dict[type[Any], JsonDict | Callable[[Any, set[int]], JsonDict]] = {
        bytes: _bytes_schema,
        bytearray: _bytearray_schema,
        Enum: _enum_schema,
        Decimal: {'type': 'string', 'format': 'decimal', 'x-python-datatype': 'Decimal'},
        datetime.datetime: {'type': 'string', 'format': 'date-time', 'x-python-datatype': 'datetime'},
        datetime.date: {'type': 'string', 'format': 'date', 'x-python-datatype': 'date'},
        datetime.time: {'type': 'string', 'format': 'time', 'x-python-datatype': 'time'},
        datetime.timedelta: {'type': 'string', 'x-python-datatype': 'timedelta'},
        GeneratorType: _generator_schema,
        IPv4Address: {'type': 'string', 'format': 'ipv4'},
        IPv6Address: {'type': 'string', 'format': 'ipv6'},
        IPv4Interface: {'type': 'string', 'format': 'ipv4interface'},
        IPv6Interface: {'type': 'string', 'format': 'ipv6interface'},
        IPv4Network: {'type': 'string', 'format': 'ipv4network'},
        IPv6Network: {'type': 'string', 'format': 'ipv6network'},
        re.Pattern: {'type': 'string', 'format': 'regex'},
        uuid.UUID: {'type': 'string', 'format': 'uuid'},
        range: {'type': 'array', 'x-python-datatype': 'range'},
        PosixPath: {'type': 'string', 'format': 'path', 'x-python-datatype': 'PosixPath'},
        Exception: _exception_schema,
    }
    with contextlib.suppress(ModuleNotFoundError):
        import pydantic

        lookup.update(
            {
                pydantic.NameEmail: {'type': 'string', 'x-python-datatype': 'NameEmail'},
                pydantic.SecretStr: {'type': 'string', 'x-python-datatype': 'SecretStr'},
                pydantic.SecretBytes: {'type': 'string', 'x-python-datatype': 'SecretBytes'},
                pydantic.AnyUrl: {'type': 'string', 'x-python-datatype': 'AnyUrl'},
                pydantic.BaseModel: _pydantic_model_schema,
                pydantic.RootModel: _pydantic_root_model_schema,
            }
        )

    with contextlib.suppress(ModuleNotFoundError):
        import pydantic_core

        lookup.update({pydantic_core.Url: {'type': 'string', 'x-python-datatype': 'Url'}})

    with contextlib.suppress(ModuleNotFoundError):
        import numpy

        lookup.update({numpy.ndarray: _numpy_schema})

    with contextlib.suppress(ModuleNotFoundError):
        import pandas

        lookup.update({pandas.DataFrame: _pandas_schema})
    return lookup


_type_to_schema = None


def create_json_schema(obj: Any, seen: set[int]) -> JsonDict:
    """Create a JSON Schema from the given object.

    Args:
        obj: The object to create the JSON Schema from.
        seen: A set of object IDs that have already been processed.

    Returns:
        The JSON Schema.
    """
    if obj is None:
        return {'type': 'null'}

    try:
        # cover common types first before calling `type_to_schema` to avoid the overhead of imports if not necessary
        obj_type = obj.__class__

        if obj_type in {str, int, bool, float}:
            return {}

        if id(obj) in seen:
            return {}

        seen = seen.copy()
        seen.add(id(obj))

        if obj_type in {list, tuple, set, frozenset, deque}:
            return _array_schema(obj, seen)
        elif isinstance(obj, Mapping):
            return _mapping_schema(obj, seen)

        sa_schema = _sqlalchemy_schema(obj, seen)
        if sa_schema:
            return sa_schema
        elif dataclasses.is_dataclass(obj) and not isinstance(obj, type):
            return _dataclass_schema(obj, seen)
        elif is_attrs(obj_type):
            return _attrs_schema(obj, seen)

        global _type_to_schema
        _type_to_schema = _type_to_schema or type_to_schema()
        for base in obj_type.__mro__[:-1]:
            try:
                schema = _type_to_schema[base]
            except KeyError:
                continue
            else:
                return schema(obj, seen) if callable(schema) else schema

        # cover subclasses of common types, can't come earlier due to conflicts with IntEnum and StrEnum
        if isinstance(obj, (str, int, float)):
            return {}
        elif isinstance(obj, Sequence):
            return {'type': 'array', 'title': obj_type.__name__, 'x-python-datatype': 'Sequence'}
    except Exception:  # pragma: no cover
        log_internal_error()

    return {'type': 'object', 'x-python-datatype': 'unknown'}


JsonSchemaProperties = NewType('JsonSchemaProperties', JsonDict)


# The value of the span attribute with key ATTRIBUTES_JSON_SCHEMA_KEY,
# representing the JSON schema of the user-defined attributes.
def attributes_json_schema(properties: JsonSchemaProperties) -> str:
    return dump_json({'type': 'object', 'properties': properties})


# This becomes the value of `properties` above.
def attributes_json_schema_properties(attributes: dict[str, Any]) -> JsonSchemaProperties:
    return JsonSchemaProperties(
        {key: create_json_schema(value, set()) for key, value in attributes.items() if key not in EXCLUDE_KEYS}
    )


# Attributes from STACK_INFO_KEYS are merged with the logfire function attributes on
# `install_auto_tracing` and when using our stdlib logging handler. We need to remove them
# from the JSON Schema, as we only want to have the ones that the user passes in.
# ATTRIBUTES_SCRUBBED_KEY can be set when formatting a message.
EXCLUDE_KEYS = STACK_INFO_KEYS | {ATTRIBUTES_SCRUBBED_KEY}


def _dataclass_schema(obj: Any, seen: set[int]) -> JsonDict:
    # NOTE: The `x-python-datatype` is "dataclass" for both standard dataclasses and Pydantic dataclasses.
    # We don't need to distinguish between them on the frontend, or to reconstruct the type on the JSON formatter.
    return _custom_object_schema(
        obj, 'dataclass', (field.name for field in dataclasses.fields(obj) if field.repr), seen
    )


def _bytes_schema(obj: bytes, _seen: set[int]) -> JsonDict:
    schema: JsonDict = {'type': 'string', 'x-python-datatype': 'bytes'}
    if obj.__class__.__name__ != 'bytes':
        schema['title'] = obj.__class__.__name__
    return schema


def _bytearray_schema(obj: bytearray, _seen: set[int]) -> JsonDict:
    schema: JsonDict = {'type': 'string', 'x-python-datatype': 'bytearray'}
    # TODO(Marcelo): We should add a test for the following branch.
    if obj.__class__.__name__ != 'bytearray':  # pragma: no cover
        schema['title'] = obj.__class__.__name__
    return schema


# NOTE: We don't handle enums where members are not basic types (str, int, bool, float) very well.
# The "type" will always be "object".
def _enum_schema(obj: Enum, seen: set[int]) -> JsonDict:
    enum_values = [e.value for e in obj.__class__]
    enum_types = set(type(value).__name__ for value in enum_values)
    if all(t in {'str', 'int', 'bool', 'float'} for t in enum_types):
        type_ = {'str': 'string', 'int': 'integer', 'bool': 'boolean', 'float': 'number'}[enum_types.pop()]
    else:
        type_ = 'object'

    return {
        'type': type_,
        'title': obj.__class__.__name__,
        'x-python-datatype': 'Enum',
        'enum': to_json_value(enum_values, seen),
    }


# Schemas for values that are already JSON serializable, i.e. that don't need to be included
# (except at the top level) because the frontend can just render them as plain JSON.
PLAIN_SCHEMAS: tuple[JsonDict, ...] = ({}, {'type': 'object'}, {'type': 'array'}, {'type': 'null'})


def _mapping_schema(obj: Any, seen: set[int]) -> JsonDict:
    obj = cast(Mapping[Any, Any], obj)
    schema: JsonDict = {
        'type': 'object',
        **_properties({(k if isinstance(k, str) else safe_repr(k)): v for k, v in obj.items()}, seen),
    }
    if obj.__class__.__name__ != 'dict':
        schema['x-python-datatype'] = 'Mapping'
        schema['title'] = obj.__class__.__name__
    return schema


def _array_schema(
    obj: list[Any] | tuple[Any, ...] | deque[Any] | set[Any] | frozenset[Any], seen: set[int]
) -> JsonDict:
    schema: dict[str, Any] = {'type': 'array'}
    if type(obj) != list:  # noqa: E721
        schema['x-python-datatype'] = obj.__class__.__name__

    if isinstance(obj, (set, frozenset)):
        try:
            obj = sorted(obj)
        except TypeError:
            return schema
    prefix_items: list[Any] = []

    if len(obj) == 0:
        return schema

    previous_schema: dict[str, Any] | None = None
    use_items_key = True
    found_non_empty_schema = False
    for item in obj:
        item_schema = create_json_schema(item, seen)
        prefix_items.append(item_schema)

        if previous_schema is not None and item_schema != previous_schema:
            use_items_key = False
        if item_schema not in PLAIN_SCHEMAS:
            found_non_empty_schema = True

        previous_schema = item_schema

    if found_non_empty_schema:
        if use_items_key:
            schema['items'] = previous_schema
        else:
            schema['prefixItems'] = prefix_items
    return schema


def _generator_schema(obj: GeneratorType[Any, Any, Any], _seen: set[int]) -> JsonDict:
    return {'type': 'array', 'x-python-datatype': 'generator', 'title': obj.__class__.__name__}


def _exception_schema(obj: Exception, _seen: set[int]) -> JsonDict:
    return {'type': 'object', 'title': obj.__class__.__name__, 'x-python-datatype': 'Exception'}


def _pydantic_root_model_schema(obj: Any, seen: set[int]) -> JsonDict:
    import pydantic

    assert isinstance(obj, pydantic.RootModel)

    root = obj.root  # type: ignore

    if isinstance(root, type(None)):
        return {'type': 'null'}

    schema: JsonDict = {}

    # return a complex schema to ensure JSON parsing for simple objects inside RootModel since they get an
    # extra layer of JSON encoding and to handle subclass representations correctly
    primitive_types = (str, bool, int, float)
    if isinstance(root, primitive_types):
        datatype = None
        if isinstance(root, str):
            type_ = 'string'
            datatype = 'str'
        elif isinstance(root, bool):
            type_ = 'boolean'
        elif isinstance(root, int):
            type_ = 'integer'
            datatype = 'int'
        else:
            type_ = 'number'
            datatype = 'float'

        schema = {'type': type_, 'x-python-datatype': datatype}
        if root.__class__ not in primitive_types:
            schema['title'] = root.__class__.__name__

        return schema

    return create_json_schema(obj.root, seen)  # type: ignore


def _pydantic_model_schema(obj: Any, seen: set[int]) -> JsonDict:
    import pydantic

    assert isinstance(obj, pydantic.BaseModel)

    try:
        fields = type(obj).model_fields
        extra = obj.model_extra or {}
    except AttributeError:  # pragma: no cover
        # pydantic v1
        fields = obj.__fields__  # type: ignore
        extra = {}
    return _custom_object_schema(obj, 'PydanticModel', [*fields, *extra], seen)


def _pandas_schema(obj: Any, _seen: set[int]) -> JsonDict:
    import pandas

    assert isinstance(obj, pandas.DataFrame)

    row_count, column_count = obj.shape

    max_columns = pandas.get_option('display.max_columns')
    col_middle = min(max_columns, column_count) // 2
    columns = list(obj.columns[:col_middle]) + list(obj.columns[-col_middle:])  # type: ignore

    max_rows = pandas.get_option('display.max_rows')
    row_middle = min(max_rows, row_count) // 2
    indices = list(obj.index[:row_middle]) + list(obj.index[-row_middle:])  # type: ignore

    return {
        'type': 'array',
        'x-python-datatype': 'DataFrame',
        'x-columns': columns,
        'x-column-count': column_count,
        'x-indices': indices,
        'x-row-count': row_count,
    }


def _numpy_schema(obj: Any, seen: set[int]) -> JsonDict:
    import numpy

    assert isinstance(obj, numpy.ndarray)

    return {
        'type': 'array',
        'x-python-datatype': 'ndarray',
        'x-shape': to_json_value(obj.shape, seen),  # type: ignore
        'x-dtype': str(obj.dtype),  # type: ignore
    }


def _attrs_schema(obj: Any, seen: set[int]) -> JsonDict:
    import attrs

    obj = cast(attrs.AttrsInstance, obj)
    return _custom_object_schema(obj, 'attrs', (key.name for key in obj.__attrs_attrs__), seen)


def _sqlalchemy_schema(obj: Any, seen: set[int]) -> JsonDict | None:
    if not is_sqlalchemy(obj):
        return None

    from sqlalchemy import exc, inspect as sa_inspect

    try:
        state = sa_inspect(obj)
    except exc.NoInspectionAvailable:
        return None
    keys = [key for key in sa_inspect(obj).attrs.keys() if key not in state.unloaded]
    return _custom_object_schema(obj, 'sqlalchemy', keys, seen)


def _properties(properties: dict[str, Any], seen: set[int]) -> JsonDict:
    schema_properties: JsonDict = {}
    for key, value in properties.items():
        if (value_schema := create_json_schema(value, seen)) not in PLAIN_SCHEMAS:
            schema_properties[key] = value_schema

    if schema_properties:
        return {'properties': schema_properties}
    else:
        return {}


def _custom_object_schema(obj: Any, datatype_name: str, keys: Iterable[str], seen: set[int]) -> JsonDict:
    properties: dict[str, Any] = {}
    for key in keys:
        with suppress(Exception):
            properties[key] = getattr(obj, key)
    return {
        'type': 'object',
        'title': obj.__class__.__name__,
        'x-python-datatype': datatype_name,
        **_properties(properties, seen),
    }
