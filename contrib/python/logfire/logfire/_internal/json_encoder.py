from __future__ import annotations

import contextlib
import dataclasses
import datetime
import json
from collections.abc import Mapping, Sequence
from decimal import Decimal
from enum import Enum
from functools import cache, lru_cache
from ipaddress import IPv4Address, IPv4Interface, IPv4Network, IPv6Address, IPv6Interface, IPv6Network
from itertools import chain
from pathlib import PosixPath
from re import Pattern
from types import GeneratorType
from typing import Any, Callable
from uuid import UUID

from .utils import JsonValue, safe_repr

NUMPY_DIMENSION_MAX_SIZE = 10
"""The maximum size of a dimension of a numpy array."""


def _bytes_encoder(o: bytes, _seen: set[int]) -> str:
    """Encode bytes using repr() to get a string representation of the bytes object.

    We remove the leading 'b' and the quotes around the string representation.

    Examples:
        >>> print(b'hello')
        b'hello'
        >>> print(_bytes_encoder(b'hello'))
        hello
    """
    return repr(o)[2:-1]


def _bytearray_encoder(o: bytearray, seen: set[int]) -> str:
    return _bytes_encoder(bytes(o), seen)


def _set_encoder(o: set[Any], seen: set[int]) -> JsonValue:
    try:
        return [to_json_value(item, seen) for item in sorted(o)]
    except TypeError:
        return [to_json_value(item, seen) for item in o]


def _to_isoformat(o: Any, _seen: set[int]) -> str:
    return o.isoformat()


def _to_str(o: Any, _seen: set[int]) -> str:
    return str(o)


def _to_repr(o: Any, _seen: set[int]) -> str:
    return repr(o)


def _pandas_data_frame_encoder(o: Any, seen: set[int]) -> JsonValue:
    """Encode pandas data frame by extracting important information.

    It summarizes rows and columns if they are more than limit.
    e.g. The data part of a data frame like:
    [
        [1, 2, 3, 4, 5],
        [2, 3, 6, 8, 10],
        [3, 6, 9, 12, 15],
        [4, 8, 12, 16, 20],
        [5, 10, 15, 20, 25],
    ]
    will be summarized to:
    [
        [1, 2, 4, 5],
        [2, 3, 8, 10],
        [4, 8, 16, 20],
        [5, 10, 20, 25],
    ]
    """
    import pandas

    max_rows = pandas.get_option('display.max_rows')
    max_columns = pandas.get_option('display.max_columns')

    col_middle = max_columns // 2
    column_count = len(o.columns)

    rows: list[Any] = []
    row_count = len(o)

    if row_count > max_rows:
        row_middle = max_rows // 2
        df_rows = chain(o.head(row_middle).iterrows(), o.tail(row_middle).iterrows())
    else:
        df_rows = o.iterrows()

    for _, row in df_rows:
        if column_count > max_columns:
            rows.append(list(row[:col_middle]) + list(row[-col_middle:]))
        else:
            rows.append(list(row))

    return to_json_value(rows, seen)


def _numpy_array_encoder(o: Any, seen: set[int]) -> JsonValue:
    """Encode numpy array by extracting important information.

    It summarizes rows and columns if they are more than limit.
    e.g. The data part of a data frame like:
    [
        [1, 2, 3, 4, 5],
        [2, 3, 6, 8, 10],
        [3, 6, 9, 12, 15],
        [4, 8, 12, 16, 20],
        [5, 10, 15, 20, 25],
    ]
    will be summarized to:
    [
        [1, 2, 4, 5],
        [2, 3, 8, 10],
        [4, 8, 16, 20],
        [5, 10, 20, 25],
    ]
    """
    # If we reach here, numpy is installed.
    import numpy

    shape = o.shape
    dimensions = o.ndim

    if isinstance(o, numpy.matrix):
        o = o.A  # type: ignore[reportUnknownMemberType]

    for dimension in range(dimensions):
        if shape[dimension] <= NUMPY_DIMENSION_MAX_SIZE:
            continue
        # In case of multiple dimensions, we limit the dimension size by the NUMPY_DIMENSION_MAX_SIZE.
        half = NUMPY_DIMENSION_MAX_SIZE // 2
        # Slicing and concatenating arrays along the specified axis
        slices = [slice(None)] * dimensions
        slices[dimension] = slice(0, half)
        front = o[tuple(slices)]

        slices[dimension] = slice(-half, None)
        end = o[tuple(slices)]
        o = numpy.concatenate((front, end), axis=dimension)

    return to_json_value(o.tolist(), seen)


def _pydantic_model_encoder(o: Any, seen: set[int]) -> JsonValue:
    import pydantic

    assert isinstance(o, pydantic.BaseModel)

    if isinstance(o, pydantic.RootModel):
        return to_json_value(o.root, seen)  # type: ignore

    try:
        dump = o.model_dump()
    except AttributeError:  # pragma: no cover
        # pydantic v1
        dump = o.dict()  # type: ignore
    return to_json_value(dump, seen)


def _get_sqlalchemy_data(o: Any, seen: set[int]) -> JsonValue | None:
    if not is_sqlalchemy(o):
        return None
    try:
        from sqlalchemy import exc, inspect as sa_inspect

        try:
            state = sa_inspect(o)
        except exc.NoInspectionAvailable:
            return None
        deferred = state.unloaded
    except ModuleNotFoundError:  # pragma: no cover
        deferred = set()  # type: ignore

    return to_json_value(
        {field: getattr(o, field) if field not in deferred else '<deferred>' for field in o.__mapper__.attrs.keys()},
        seen,
    )


EncoderFunction = Callable[[Any, 'set[int]'], JsonValue]


@cache
def encoder_by_type() -> dict[type[Any], EncoderFunction]:
    lookup: dict[type[Any], EncoderFunction] = {
        set: _set_encoder,
        frozenset: _set_encoder,
        bytes: _bytes_encoder,
        bytearray: _bytearray_encoder,
        datetime.date: _to_isoformat,
        datetime.datetime: _to_isoformat,
        datetime.time: _to_isoformat,
        datetime.timedelta: lambda o, _: o.total_seconds(),
        Decimal: _to_str,
        Enum: lambda o, seen: to_json_value(o.value, seen),
        GeneratorType: _to_repr,
        IPv4Address: _to_str,
        IPv4Interface: _to_str,
        IPv4Network: _to_str,
        IPv6Address: _to_str,
        IPv6Interface: _to_str,
        IPv6Network: _to_str,
        PosixPath: _to_str,
        Pattern: lambda o, seen: to_json_value(o.pattern, seen),
        UUID: _to_str,
        Exception: _to_str,
    }
    with contextlib.suppress(ModuleNotFoundError):
        import pydantic
        import pydantic_core

        lookup.update(
            {
                pydantic.AnyUrl: _to_str,
                pydantic_core.Url: _to_str,
                pydantic.NameEmail: _to_str,
                pydantic.SecretBytes: _to_str,
                pydantic.SecretStr: _to_str,
                pydantic.BaseModel: _pydantic_model_encoder,
            }
        )

    with contextlib.suppress(ModuleNotFoundError):
        import pandas

        lookup.update({pandas.DataFrame: _pandas_data_frame_encoder})
    with contextlib.suppress(ModuleNotFoundError):
        import numpy

        lookup.update({numpy.ndarray: _numpy_array_encoder})

    return lookup


def to_json_value(o: Any, seen: set[int]) -> JsonValue:
    try:
        if isinstance(o, (int, float, str, bool, type(None))):
            return o

        if id(o) in seen:
            return '<circular reference>'

        seen = seen.copy()
        seen.add(id(o))

        if isinstance(o, (list, tuple)):
            # we do list & tuple before Mapping as it's > twice as fast and just as common
            return [to_json_value(item, seen) for item in o]  # type: ignore
        elif isinstance(o, Mapping):
            return {
                key if isinstance(key, str) else safe_repr(key): to_json_value(value, seen)
                for key, value in o.items()  # type: ignore
            }

        sa_data = _get_sqlalchemy_data(o, seen)
        if sa_data is not None:
            return sa_data
        elif dataclasses.is_dataclass(o):
            return {f.name: to_json_value(getattr(o, f.name), seen) for f in dataclasses.fields(o) if f.repr}
        elif is_attrs(o.__class__):
            return _get_attrs_data(o, seen)

        # Check the class type and its superclasses for a matching encoder
        for base in o.__class__.__mro__[:-1]:
            try:
                encoder = encoder_by_type()[base]
            except KeyError:
                pass
            else:
                return encoder(o, seen)

        if isinstance(o, Sequence):
            return [to_json_value(item, seen) for item in o]  # type: ignore

        try:
            # Some VertexAI classes have this method. They have no common base class.
            # Seems like a sensible thing to try in general.
            to_dict = type(o).to_dict(o)  # type: ignore
        except Exception:  # currently redundant, but future-proof
            pass
        else:
            return to_json_value(to_dict, seen)

    except Exception:  # pragma: no cover
        pass

    # In case we don't know how to encode, use `repr()`.
    return safe_repr(o)


def logfire_json_dumps(obj: Any) -> str:
    try:
        return json.dumps(obj, default=lambda o: to_json_value(o, set()), separators=(',', ':'), ensure_ascii=False)
    except Exception:
        # fallback to eagerly calling to_json_value to take care of object keys which are not strings
        # see https://github.com/pydantic/platform/pull/2045
        return json.dumps(to_json_value(obj, set()), separators=(',', ':'), ensure_ascii=False)


def is_sqlalchemy(obj: Any) -> bool:
    try:
        if not hasattr(obj, '__mapper__'):
            # A SQLModel without `table=True` will pass `isinstance(obj.__class__, DeclarativeMeta)` (I don't know how)
            # but will fail when retrieving data, specifically when calling `sqlalchemy.inspect`
            # or when getting the `__mapper__` attribute.
            return False

        from sqlalchemy.orm import DeclarativeBase, DeclarativeMeta

        return isinstance(obj, DeclarativeBase) or isinstance(obj.__class__, DeclarativeMeta)
    except Exception:
        return False


@lru_cache
def is_attrs(cls: type) -> bool:
    try:
        import attrs

        return attrs.has(cls)
    except ModuleNotFoundError:  # pragma: no cover
        return False


def _get_attrs_data(o: Any, seen: set[int]) -> JsonValue:
    import attrs

    return {f.name: to_json_value(getattr(o, f.name), seen) for f in attrs.fields(o.__class__)}
