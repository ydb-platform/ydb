"""
Support for composite types adaptation.
"""

# Copyright (C) 2020 The Psycopg Team

from __future__ import annotations

import re
import struct
import logging
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Any, Generic, NamedTuple, TypeAlias, TypeVar, cast
from functools import cache
from collections import namedtuple
from collections.abc import Callable, Sequence

from .. import abc, postgres, pq, sql
from .._oids import TEXT_OID
from ..adapt import Buffer, Dumper, Loader, PyFormat, RecursiveDumper, RecursiveLoader
from ..adapt import Transformer
from .._struct import pack_len, unpack_len
from .._typeinfo import TypeInfo
from .._encodings import _as_python_identifier

if TYPE_CHECKING:
    from .._connection_base import BaseConnection

logger = logging.getLogger("psycopg")

_struct_oidlen = struct.Struct("!Ii")
_pack_oidlen = cast(Callable[[int, int], bytes], _struct_oidlen.pack)
_unpack_oidlen = cast(
    Callable[[abc.Buffer, int], "tuple[int, int]"], _struct_oidlen.unpack_from
)

T = TypeVar("T")
ObjectMaker: TypeAlias = Callable[[Sequence[Any], "CompositeInfo"], T]
SequenceMaker: TypeAlias = Callable[[T, "CompositeInfo"], Sequence[Any]]


class CompositeInfo(TypeInfo):
    """Manage information about a composite type."""

    def __init__(
        self,
        name: str,
        oid: int,
        array_oid: int,
        *,
        regtype: str = "",
        field_names: Sequence[str],
        field_types: Sequence[int],
    ):
        super().__init__(name, oid, array_oid, regtype=regtype)
        self.field_names = tuple(field_names)
        self.field_types = tuple(field_types)
        # Will be set by register() if the `factory` is a type
        self.python_type: type | None = None

    def __hash__(self) -> int:
        return hash((self.name, self.field_names, self.field_types))

    @classmethod
    def _get_info_query(cls, conn: BaseConnection[Any]) -> abc.QueryNoTemplate:
        return sql.SQL("""\
SELECT
    t.typname AS name, t.oid AS oid, t.typarray AS array_oid,
    t.oid::regtype::text AS regtype,
    coalesce(a.fnames, '{{}}') AS field_names,
    coalesce(a.ftypes, '{{}}') AS field_types
FROM pg_type t
LEFT JOIN (
    SELECT
        attrelid,
        array_agg(attname) AS fnames,
        array_agg(atttypid) AS ftypes
    FROM (
        SELECT a.attrelid, a.attname, a.atttypid
        FROM pg_attribute a
        JOIN pg_type t ON t.typrelid = a.attrelid
        WHERE t.oid = {regtype}
        AND a.attnum > 0
        AND NOT a.attisdropped
        ORDER BY a.attnum
    ) x
    GROUP BY attrelid
) a ON a.attrelid = t.typrelid
WHERE t.oid = {regtype}
""").format(regtype=cls._to_regtype(conn))


class TupleDumper(RecursiveDumper):
    # Should be this, but it doesn't work
    # oid = _oids.RECORD_OID

    def dump(self, obj: tuple[Any, ...]) -> Buffer | None:
        return _dump_text_sequence(obj, self._tx)


class _SequenceDumper(RecursiveDumper, Generic[T], ABC):
    """
    Base class for text dumpers taking an object and dumping it as a composite.

    Abstract class: subclasses must specify the names of the fields making the
    composite to return and the `make_sequence()` static method to convert the
    object to dump to a sequence of values.
    """

    # Subclasses must set this attribute
    info: CompositeInfo

    def dump(self, obj: T) -> bytes:
        seq = type(self).make_sequence(obj, self.info)
        return _dump_text_sequence(seq, self._tx)

    @staticmethod
    @abstractmethod
    def make_sequence(obj: T, info: CompositeInfo) -> Sequence[Any]: ...


class _SequenceBinaryDumper(Dumper, Generic[T], ABC):
    """
    Base class for binary dumpers taking an object and dumping it as a composite.

    Abstract class: subclasses must specify the names and types of the fields
    making the target composite and the `make_sequence()` static method to
    convert the object to dump to a sequence of values.
    """

    format = pq.Format.BINARY
    # Subclasses must set this attribute
    info: CompositeInfo

    def __init__(self, cls: type[T], context: abc.AdaptContext | None = None):
        super().__init__(cls, context)

        # Note: this class is not a RecursiveDumper because it would use the
        # same Transformer of the context, which would confuse dump_sequence()
        # in case the composite contains another composite. Make sure to use
        # a separate Transformer instance instead.
        self._tx = Transformer(context)
        self._tx.set_dumper_types(self.info.field_types, self.format)

        nfields = len(self.info.field_types)
        self._formats = (PyFormat.from_pq(self.format),) * nfields

    def dump(self, obj: T) -> Buffer | None:
        seq = type(self).make_sequence(obj, self.info)
        return _dump_binary_sequence(
            seq, self.info.field_types, self._formats, self._tx
        )

    @staticmethod
    @abstractmethod
    def make_sequence(obj: T, info: CompositeInfo) -> Sequence[Any]: ...


class RecordLoader(RecursiveLoader):
    """
    Load a `record` field from PostgreSQL.

    In text mode we don't have type information of the composite's fields, so
    convert every item as text. Note that in binary loading we have per-field
    oids instead.
    """

    def load(self, data: abc.Buffer) -> tuple[Any, ...]:
        if data == b"()":
            return ()

        cast = self._tx.get_loader(TEXT_OID, self.format).load
        record = _parse_text_record(data[1:-1])
        for i in range(len(record)):
            if (f := record[i]) is not None:
                record[i] = cast(f)

        return tuple(record)


class RecordBinaryLoader(Loader):
    """
    Load a `record` field from PostgreSQL.

    Unlike in text mode, the composite data contains oids of the fields,
    so we can actually parse the records in its original types.
    """

    format = pq.Format.BINARY

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        super().__init__(oid, context)
        self._ctx = context
        # Cache a transformer for each sequence of oid found.
        # Usually there will be only one, but if there is more than one
        # row in the same query (in different columns, or even in different
        # records), oids might differ and we'd need separate transformers.
        self._txs: dict[tuple[int, ...], abc.Transformer] = {}

    def load(self, data: abc.Buffer) -> tuple[Any, ...]:
        record, oids = _parse_binary_record(data)
        if not record:
            return ()

        tx = self._get_transformer(tuple(oids))
        return tx.load_sequence(record)

    def _get_transformer(self, key: tuple[int, ...]) -> abc.Transformer:
        if key in self._txs:
            return self._txs[key]

        tx = Transformer(self._ctx)
        tx.set_loader_types([*key], self.format)
        self._txs[key] = tx
        return tx


class _CompositeLoader(Loader, Generic[T], ABC):
    """
    Base class to create text loaders of specific composite types.

    The class is complete but lack information about the fields types and
    object factory. These will be added by register_composite(), which will
    create a subclass of this class.
    """

    # Subclasses must set this attribute
    info: CompositeInfo

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        super().__init__(oid, context)
        # Note: we cannot use the RecursiveLoader base class here because we
        # always want a different Transformer instance, otherwise the types
        # loaded will conflict with the types loaded by the record.
        self._tx = Transformer(context)
        self._tx.set_loader_types(self.info.field_types, self.format)

    def load(self, data: abc.Buffer) -> T:
        if data == b"()":
            args = ()
        else:
            args = self._tx.load_sequence(tuple(_parse_text_record(data[1:-1])))
        return type(self).make_object(args, self.info)

    @staticmethod
    @abstractmethod
    def make_object(args: Sequence[Any], info: CompositeInfo) -> T: ...


class _CompositeBinaryLoader(Loader, Generic[T], ABC):
    """
    Base class to create text loaders of specific composite types.

    The class is complete but lack information about the fields types, names,
    and object factory. These will be added by register_composite(), which will
    create a subclass of this class.
    """

    format = pq.Format.BINARY
    # Subclasses must set this attribute
    info: CompositeInfo

    def __init__(self, oid: int, context: abc.AdaptContext | None = None):
        super().__init__(oid, context)
        self._tx = Transformer(context)
        self._tx.set_loader_types(self.info.field_types, self.format)

    def load(self, data: abc.Buffer) -> T:
        brecord, _ = _parse_binary_record(data)  # assume oids == self.fields_types
        record = self._tx.load_sequence(brecord)
        return type(self).make_object(record, self.info)

    @staticmethod
    @abstractmethod
    def make_object(args: Sequence[Any], info: CompositeInfo) -> T: ...


def register_composite(
    info: CompositeInfo,
    context: abc.AdaptContext | None = None,
    factory: Callable[..., T] | None = None,
    *,
    make_object: ObjectMaker[T] | None = None,
    make_sequence: SequenceMaker[T] | None = None,
) -> None:
    """Register the adapters to load and dump a composite type.

    :param info: The object with the information about the composite to register.
    :type info: `CompositeInfo`
    :param context: The context where to register the adapters. If `!None`,
        register it globally.
    :type context: `~psycopg.abc.AdaptContext` | `!None`
    :param factory: Callable to create a Python object from the sequence of
        attributes read from the composite.
    :type factory: `!Callable[..., T]` | `!None`
    :param make_object: optional function that will be used when loading a
        composite type from the database if the Python type is not a sequence
        compatible with the composite fields
    :type make_object: `!Callable[[Sequence[Any], CompositeInfo], T]` | `!None`
    :param make_sequence: optional function that will be used when dumping an
        object to the database if the object is not a sequence compatible
        with the composite fields
    :type make_sequence: `!Callable[[T, CompositeInfo], Sequence[Any]]` | `!None`

    .. note::

        Registering the adapters doesn't affect objects already created, even
        if they are children of the registered context. For instance,
        registering the adapter globally doesn't affect already existing
        connections.
    """

    # A friendly error warning instead of an AttributeError in case fetch()
    # failed and it wasn't noticed.
    if not info:
        raise TypeError("no info passed. Is the requested composite available?")

    # Register arrays and type info
    info.register(context)

    if not factory:
        factory = cast("Callable[..., T]", _nt_from_info(info))

    if not make_object:

        def make_object(values: Sequence[Any], info: CompositeInfo) -> T:
            return factory(*values)

    adapters = context.adapters if context else postgres.adapters

    # generate and register a customized text loader
    loader: type[Loader] = _make_loader(info, make_object)
    adapters.register_loader(info.oid, loader)

    # generate and register a customized binary loader
    loader = _make_binary_loader(info, make_object)
    adapters.register_loader(info.oid, loader)

    # If the factory is a type, create and register dumpers for it
    if isinstance(factory, type):

        # Optimistically assume that the factory type is a sequence.
        # If it is not, it will create a non-functioning dumper, but we don't
        # risk backward incompatibility.
        if not make_sequence:

            if not issubclass(factory, Sequence):
                logger.warning(
                    "the type %r is not a sequence: dumping these objects to the"
                    " database will fail. Please specify a `make_sequence`"
                    " argument in the `register_composite()` call",
                    factory.__name__,
                )

                def make_sequence(obj: T, into: CompositeInfo) -> Sequence[Any]:
                    raise TypeError(
                        f"{type(obj).__name__!r} objects cannot be dumped without"
                        " specifying 'make_sequence' in 'register_composite()'"
                    )

            else:

                def make_sequence(obj: T, info: CompositeInfo) -> Sequence[Any]:
                    return obj  # type: ignore[return-value]  # it's a sequence

        type_name = factory.__name__
        dumper: type[Dumper] = _make_binary_dumper(type_name, info, make_sequence)
        adapters.register_dumper(factory, dumper)

        # Default to the text dumper because it is more flexible
        dumper = _make_dumper(type_name, info, make_sequence)
        adapters.register_dumper(factory, dumper)

        info.python_type = factory

    else:
        if make_sequence:
            raise TypeError(
                "the factory {factory.__name__!r} is not a type: you cannot"
                " create a dumper by specifying `make_sequence`."
            )


def register_default_adapters(context: abc.AdaptContext) -> None:
    adapters = context.adapters
    adapters.register_dumper(tuple, TupleDumper)
    adapters.register_loader("record", RecordLoader)
    adapters.register_loader("record", RecordBinaryLoader)


@cache
def _nt_from_info(info: CompositeInfo) -> type[NamedTuple]:
    name = _as_python_identifier(info.name)
    fields = tuple(_as_python_identifier(n) for n in info.field_names)
    return _make_nt(name, fields)


def _dump_text_sequence(seq: Sequence[Any], tx: abc.Transformer) -> bytes:
    if not seq:
        return b"()"

    parts: list[abc.Buffer] = [b"("]

    for item in seq:
        if item is None:
            parts.append(b",")
            continue

        dumper = tx.get_dumper(item, PyFormat.TEXT)
        if (ad := dumper.dump(item)) is None:
            ad = b""
        elif not ad:
            ad = b'""'
        elif _re_needs_quotes.search(ad):
            ad = b'"' + _re_esc.sub(rb"\1\1", ad) + b'"'

        parts.append(ad)
        parts.append(b",")

    parts[-1] = b")"

    return b"".join(parts)


_re_needs_quotes = re.compile(rb'[",\\\s()]')
_re_esc = re.compile(rb"([\\\"])")


def _dump_binary_sequence(
    seq: Sequence[Any],
    types: Sequence[int],
    formats: Sequence[PyFormat],
    tx: abc.Transformer,
) -> bytearray:
    out = bytearray(pack_len(len(seq)))
    adapted = tx.dump_sequence(seq, formats)
    for i in range(len(seq)):
        b = adapted[i]
        oid = types[i]
        if b is not None:
            out += _pack_oidlen(oid, len(b))
            out += b
        else:
            out += _pack_oidlen(oid, -1)

    return out


def _parse_text_record(data: abc.Buffer) -> list[bytes | None]:
    """
    Split a non-empty representation of a composite type into components.

    Terminators shouldn't be used in `!data` (so that both record and range
    representations can be parsed).
    """
    record: list[bytes | None] = []
    for m in _re_tokenize.finditer(data):
        if m.group(1):
            record.append(None)
        elif m.group(2) is not None:
            record.append(_re_undouble.sub(rb"\1", m.group(2)))
        else:
            record.append(m.group(3))

    # If the final group ended in `,` there is a final NULL in the record
    # that the regexp couldn't parse.
    if m and m.group().endswith(b","):
        record.append(None)

    return record


_re_tokenize = re.compile(rb"""(?x)
      (,)                       # an empty token, representing NULL
    | " ((?: [^"] | "")*) " ,?  # or a quoted string
    | ([^",)]+) ,?              # or an unquoted string
    """)
_re_undouble = re.compile(rb'(["\\])\1')


def _parse_binary_record(data: abc.Buffer) -> tuple[list[Buffer | None], list[int]]:
    """
    Parse the binary representation of a composite type.

    Return the sequence of fields and oids found in the type. The fields
    are returned as buffer: they will need a Transformer to be converted
    to Python types.
    """
    nfields = unpack_len(data, 0)[0]
    offset = 4
    oids = []
    record: list[Buffer | None] = []
    for _ in range(nfields):
        oid, length = _unpack_oidlen(data, offset)
        offset += 8
        oids.append(oid)
        if length >= 0:
            record.append(data[offset : offset + length])
            offset += length
        else:
            record.append(None)

    return record, oids


# Cache all dynamically-generated types to avoid leaks in case the types
# cannot be GC'd.


@cache
def _make_nt(name: str, fields: tuple[str, ...]) -> type[NamedTuple]:
    return namedtuple(name, fields)  # type: ignore[return-value]


@cache
def _make_loader(
    info: CompositeInfo, make_object: ObjectMaker[T]
) -> type[_CompositeLoader[T]]:
    doc = f"Text loader for the '{info.name}' composite."
    d = {"__doc__": doc, "info": info, "make_object": make_object}
    return type(f"{info.name.title()}Loader", (_CompositeLoader,), d)


@cache
def _make_binary_loader(
    info: CompositeInfo, make_object: ObjectMaker[T]
) -> type[_CompositeBinaryLoader[T]]:
    doc = f"Binary loader for the '{info.name}' composite."
    d = {"__doc__": doc, "info": info, "make_object": make_object}
    return type(f"{info.name.title()}BinaryLoader", (_CompositeBinaryLoader,), d)


@cache
def _make_dumper(
    name: str, info: CompositeInfo, make_sequence: SequenceMaker[T]
) -> type[_SequenceDumper[T]]:
    doc = f"Text dumper for the '{name}' composite."
    d = {"__doc__": doc, "oid": info.oid, "info": info, "make_sequence": make_sequence}
    return type(f"{name}Dumper", (_SequenceDumper,), d)


@cache
def _make_binary_dumper(
    name: str, info: CompositeInfo, make_sequence: SequenceMaker[T]
) -> type[_SequenceBinaryDumper[T]]:
    doc = f"Text dumper for the '{name}' composite."
    d = {"__doc__": doc, "oid": info.oid, "info": info, "make_sequence": make_sequence}
    return type(f"{name}BinaryDumper", (_SequenceBinaryDumper,), d)
