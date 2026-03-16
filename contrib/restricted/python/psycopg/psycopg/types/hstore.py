"""
dict to hstore adaptation
"""

# Copyright (C) 2021 The Psycopg Team

from __future__ import annotations

import re
from struct import Struct
from typing import TypeAlias
from functools import cache

from .. import errors as e
from .. import postgres
from ..pq import Format
from ..abc import AdaptContext, Buffer
from .._oids import TEXT_OID
from ..adapt import Loader, PyFormat, RecursiveDumper, RecursiveLoader
from .._typeinfo import TypeInfo
from .._encodings import conn_encoding

_re_escape = re.compile(r'(["\\])')
_re_unescape = re.compile(r"\\(.)")

_re_hstore = re.compile(
    r"""
    # hstore key:
    # a string of normal or escaped chars
    "((?: [^"\\] | \\. )*)"
    \s*=>\s* # hstore value
    (?:
        NULL # the value can be null - not caught
        # or a quoted string like the key
        | "((?: [^"\\] | \\. )*)"
    )
    (?:\s*,\s*|$) # pairs separated by comma or end of string.
""",
    re.VERBOSE,
)

_U32_STRUCT = Struct("!I")
"""Simple struct representing an unsigned 32-bit big-endian integer."""

_I2B = [i.to_bytes(4, "big") for i in range(64)]
"""Lookup list for small ints to bytes conversions."""


Hstore: TypeAlias = dict[str, str | None]


class BaseHstoreDumper(RecursiveDumper):
    def dump(self, obj: Hstore) -> Buffer | None:
        if not obj:
            return b""

        tokens: list[str] = []

        def add_token(s: str) -> None:
            tokens.append('"')
            tokens.append(_re_escape.sub(r"\\\1", s))
            tokens.append('"')

        for k, v in obj.items():
            if not isinstance(k, str):
                raise e.DataError("hstore keys can only be strings")
            add_token(k)

            tokens.append("=>")

            if v is None:
                tokens.append("NULL")
            elif not isinstance(v, str):
                raise e.DataError("hstore keys can only be strings")
            else:
                add_token(v)

            tokens.append(",")

        del tokens[-1]
        data = "".join(tokens)
        dumper = self._tx.get_dumper(data, PyFormat.TEXT)
        return dumper.dump(data)


class BaseHstoreBinaryDumper(RecursiveDumper):
    format = Format.BINARY

    def __init__(self, cls: type, context: AdaptContext | None = None):
        super().__init__(cls, context)
        enc = conn_encoding(self.connection)
        self.encoding = enc if enc != "ascii" else "utf-8"

    def dump(self, obj: Hstore) -> Buffer:
        if not obj:
            hstore_empty = b"\x00\x00\x00\x00"
            return hstore_empty

        hstore_null_marker = b"\xff\xff\xff\xff"
        i2b = _I2B
        encoding = self.encoding
        buffer: list[bytes] = [i2b[i] if (i := len(obj)) < 64 else i.to_bytes(4, "big")]

        for key, value in obj.items():
            key_bytes = key.encode(encoding)
            buffer.append(
                i2b[i] if (i := len(key_bytes)) < 64 else i.to_bytes(4, "big")
            )
            buffer.append(key_bytes)

            if value is None:
                buffer.append(hstore_null_marker)
            else:
                value_bytes = value.encode(encoding)
                buffer.append(
                    i2b[i] if (i := len(value_bytes)) < 64 else i.to_bytes(4, "big")
                )
                buffer.append(value_bytes)

        return b"".join(buffer)


class HstoreLoader(RecursiveLoader):
    def load(self, data: Buffer) -> Hstore:
        loader = self._tx.get_loader(TEXT_OID, self.format)
        s: str = loader.load(data)

        rv: Hstore = {}
        start = 0
        for m in _re_hstore.finditer(s):
            if m is None or m.start() != start:
                raise e.DataError(f"error parsing hstore pair at char {start}")
            k = _re_unescape.sub(r"\1", m.group(1))
            if (v := m.group(2)) is not None:
                v = _re_unescape.sub(r"\1", v)

            rv[k] = v
            start = m.end()

        if start < len(s):
            raise e.DataError(f"error parsing hstore: unparsed data after char {start}")

        return rv


class HstoreBinaryLoader(Loader):
    format = Format.BINARY

    def __init__(self, oid: int, context: AdaptContext | None = None):
        super().__init__(oid, context)
        enc = conn_encoding(self.connection)
        self.encoding = enc if enc != "ascii" else "utf-8"

    def load(self, data: Buffer) -> Hstore:
        if len(data) < 12:  # Fast-path if too small to contain any data.
            return {}

        hstore_null_marker = 0xFFFFFFFF
        unpack_from = _U32_STRUCT.unpack_from
        encoding = self.encoding
        result = {}

        view = bytes(data)
        (size,) = unpack_from(view)
        pos = 4

        for _ in range(size):
            (key_size,) = unpack_from(view, pos)
            pos += 4

            key = view[pos : pos + key_size].decode(encoding)
            pos += key_size

            (value_size,) = unpack_from(view, pos)
            pos += 4

            if value_size == hstore_null_marker:
                value = None
            else:
                value = view[pos : pos + value_size].decode(encoding)
                pos += value_size

            result[key] = value

        return result


def register_hstore(info: TypeInfo, context: AdaptContext | None = None) -> None:
    """Register the adapters to load and dump hstore.

    :param info: The object with the information about the hstore type.
    :param context: The context where to register the adapters. If `!None`,
        register it globally.

    .. note::

        Registering the adapters doesn't affect objects already created, even
        if they are children of the registered context. For instance,
        registering the adapter globally doesn't affect already existing
        connections.
    """
    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError("no info passed. Is the 'hstore' extension loaded?")

    # Register arrays and type info
    info.register(context)

    adapters = context.adapters if context else postgres.adapters

    # Generate and register customized dumpers
    adapters.register_dumper(dict, _make_hstore_dumper(info.oid))
    adapters.register_dumper(dict, _make_hstore_binary_dumper(info.oid))

    # Register the loaders on the oid
    adapters.register_loader(info.oid, HstoreLoader)
    adapters.register_loader(info.oid, HstoreBinaryLoader)


# Cache all dynamically-generated types to avoid leaks in case the types
# cannot be GC'd.


@cache
def _make_hstore_dumper(oid_in: int) -> type[BaseHstoreDumper]:
    """
    Return an hstore dumper class configured using `oid_in`.

    Avoid to create new classes if the oid configured is the same.
    """

    class HstoreDumper(BaseHstoreDumper):
        oid = oid_in

    return HstoreDumper


@cache
def _make_hstore_binary_dumper(oid_in: int) -> type[BaseHstoreBinaryDumper]:
    class HstoreBinaryDumper(BaseHstoreBinaryDumper):
        oid = oid_in

    return HstoreBinaryDumper
