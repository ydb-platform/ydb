"""Recursive decoder for ClickHouse's single-value binary format.

Values stored in JSON shared data (and in a Dynamic column's shared variant) are
written by the server as::

    <binary type encoding><value in ISerialization::serializeBinary format>

See ColumnObject::serializePathAndValueIntoSharedData and
ColumnDynamic::serializeValueIntoSharedVariant.

Two things matter here:

1. The type encoding is recursive and variable length (``Array(T)`` is
   ``0x1E<nested>``), so it cannot be resolved with a single byte lookup.
2. ``serializeBinary`` is the *single value* format, which is NOT the same as
   the one-row column format for compound types.  ``Array`` writes a var_uint
   element count followed by each element's single-value encoding, whereas the
   native column format writes UInt64 offsets.  Scalar values are byte-identical
   in both formats, which is why scalar leaves delegate to the regular column
   readers.

Values are read from the driver's regular ``ResponseBuffer`` (the compiled one
when available) because the optimized column readers require that concrete
type.

Type indexes without a handler here (Enum, Decimal, FixedString, Interval,
Variant, AggregateFunction, geo types) raise ``UnsupportedBinaryTypeError`` and
the caller falls back to returning the raw bytes.  The JSON type normalizes all
of those away before they can reach shared data (Decimal to Float64, Enum and
FixedString and UUID and IP addresses to String, Map and named Tuple to nested
JSON, unnamed Tuple to Array(Dynamic)), so a decoder for them would be
untestable dead code.  Parameterless indexes are all present regardless since
each is a single table entry.
"""

from typing import Any

from clickhouse_connect.datatypes.base import ClickHouseType
from clickhouse_connect.datatypes.registry import get_from_name
from clickhouse_connect.driver.ctypes import RespBuffCls
from clickhouse_connect.driver.exceptions import StreamCompleteException
from clickhouse_connect.driver.query import QueryContext
from clickhouse_connect.driver.types import ByteSource

# Binary type index -> ClickHouse type name for parameterless types.  The
# single-value binary encoding of every entry is identical to its one-row
# native column encoding, so values decode through the regular column readers.
SCALAR_TYPE_INDEXES = {
    0x01: "UInt8",
    0x02: "UInt16",
    0x03: "UInt32",
    0x04: "UInt64",
    0x05: "UInt128",
    0x06: "UInt256",
    0x07: "Int8",
    0x08: "Int16",
    0x09: "Int32",
    0x0A: "Int64",
    0x0B: "Int128",
    0x0C: "Int256",
    0x0D: "Float32",
    0x0E: "Float64",
    0x0F: "Date",
    0x10: "Date32",
    0x11: "DateTime",
    0x15: "String",
    0x1D: "UUID",
    0x28: "IPv4",
    0x29: "IPv6",
    0x2D: "Bool",
    0x31: "BFloat16",
    0x32: "Time",
}

NOTHING = 0x00
DATETIME_TZ = 0x12
DATETIME64 = 0x13
DATETIME64_TZ = 0x14
ARRAY = 0x1E
TUPLE = 0x1F
NAMED_TUPLE = 0x20
NULLABLE = 0x23
LOW_CARDINALITY = 0x26
MAP = 0x27
DYNAMIC = 0x2B
JSON = 0x30
TIME64 = 0x34

MAX_SUB_SECOND_SCALE = 9
# Values of a zero-width type (an empty Tuple) occupy no bytes, so their
# element counts cannot be validated against the input length.  Bound them
# separately so a desynced parse fails instead of building an arbitrarily
# large result from a few input bytes.
MAX_ZERO_WIDTH_ELEMENTS = 1_000_000


class _Limits:
    """Mutable per-decode bounds.

    The zero-width budget is shared across the whole value; a per-container
    bound would multiply under nesting (each ``Array(Array(Tuple()))`` level
    could claim the full bound again from a handful of input bytes).
    """

    __slots__ = ("input_len", "zero_width_budget")

    def __init__(self, input_len: int) -> None:
        self.input_len = input_len
        self.zero_width_budget = MAX_ZERO_WIDTH_ELEMENTS


class UnsupportedBinaryTypeError(Exception):
    """Raised when the encoded type or value contains something we cannot decode."""


class _SingleBufferSource:
    """Minimal transport source feeding one in-memory chunk to a ResponseBuffer."""

    __slots__ = ("gen",)

    def __init__(self, data: bytes) -> None:
        self.gen = iter((data,))

    def close(self, ex: "Exception | None" = None) -> None:
        pass


def _byte_source(data: bytes) -> ByteSource:
    return RespBuffCls(_SingleBufferSource(data))


class _Node:
    """One node of a parsed binary type encoding."""

    __slots__ = ("kind", "child", "children", "typed_paths", "names", "ch_type", "zero_width")

    kind: str
    child: "_Node | None"
    children: "list[_Node]"
    typed_paths: "dict[str, _Node]"
    names: "list[str]"
    ch_type: "ClickHouseType | None"
    zero_width: bool

    def __init__(
        self,
        kind: str,
        child: "_Node | None" = None,
        children: "list[_Node] | None" = None,
        typed_paths: "dict[str, _Node] | None" = None,
        names: "list[str] | None" = None,
        ch_type: "ClickHouseType | None" = None,
    ) -> None:
        self.kind = kind
        self.child = child
        self.children = children if children is not None else []
        self.typed_paths = typed_paths if typed_paths is not None else {}
        self.names = names if names is not None else []
        self.ch_type = ch_type
        # A value of a zero-width node consumes no input bytes.  Every other
        # kind consumes at least one byte per value (a flag, count, type index
        # or scalar byte).
        if kind == "nothing":
            self.zero_width = True
        elif kind in ("tuple", "named_tuple"):
            self.zero_width = all(c.zero_width for c in self.children)
        else:
            self.zero_width = False


def _scalar(type_name: str) -> _Node:
    return _Node("scalar", ch_type=get_from_name(type_name))


def _quote(name: str) -> str:
    escaped = name.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _read_scale(source: ByteSource) -> int:
    scale = source.read_byte()
    if scale > MAX_SUB_SECOND_SCALE:
        raise UnsupportedBinaryTypeError(f"Sub-second scale {scale} exceeds {MAX_SUB_SECOND_SCALE}")
    return scale


def _child_of(node: _Node) -> _Node:
    """Return the element type of a node that wraps exactly one other type."""
    if node.child is None:
        raise UnsupportedBinaryTypeError(f"{node.kind} node is missing its element type")
    return node.child


def _read_encoded_type(source: ByteSource) -> _Node:
    """Parse a binary type encoding, leaving the cursor at the start of the value."""
    idx = source.read_byte()

    name = SCALAR_TYPE_INDEXES.get(idx)
    if name is not None:
        return _scalar(name)

    if idx == NOTHING:
        return _Node("nothing")

    if idx == DATETIME_TZ:
        return _scalar(f"DateTime({_quote(source.read_leb128_str())})")

    if idx == DATETIME64:
        return _scalar(f"DateTime64({_read_scale(source)})")

    if idx == DATETIME64_TZ:
        scale = _read_scale(source)
        return _scalar(f"DateTime64({scale}, {_quote(source.read_leb128_str())})")

    if idx == TIME64:
        return _scalar(f"Time64({_read_scale(source)})")

    if idx == ARRAY:
        return _Node("array", child=_read_encoded_type(source))

    if idx == NULLABLE:
        return _Node("nullable", child=_read_encoded_type(source))

    if idx == LOW_CARDINALITY:
        # LowCardinality delegates to the dictionary type's serializeBinary
        return _read_encoded_type(source)

    if idx == MAP:
        key = _read_encoded_type(source)
        value = _read_encoded_type(source)
        return _Node("map", children=[key, value])

    if idx == TUPLE:
        count = source.read_leb128()
        return _Node("tuple", children=[_read_encoded_type(source) for _ in range(count)])

    if idx == NAMED_TUPLE:
        count = source.read_leb128()
        names = []
        children = []
        for _ in range(count):
            names.append(source.read_leb128_str())
            children.append(_read_encoded_type(source))
        return _Node("named_tuple", children=children, names=names)

    if idx == DYNAMIC:
        source.read_byte()  # uint8 max_types
        return _Node("dynamic")

    if idx == JSON:
        source.read_byte()  # uint8 serialization version
        source.read_leb128()  # max_dynamic_paths (var_uint)
        source.read_byte()  # uint8 max_dynamic_types
        typed_paths = {}
        for _ in range(source.read_leb128()):
            path = source.read_leb128_str()
            typed_paths[path] = _read_encoded_type(source)
        for _ in range(source.read_leb128()):  # SKIP paths
            source.read_leb128_str()
        for _ in range(source.read_leb128()):  # SKIP REGEXP
            source.read_leb128_str()
        return _Node("json", typed_paths=typed_paths)

    raise UnsupportedBinaryTypeError(f"Unhandled binary type index 0x{idx:02X}")


def _read_element_count(source: ByteSource, zero_width: bool, limits: _Limits, what: str) -> int:
    """Read a var_uint element count, bounded so a desynced or corrupt buffer
    fails fast instead of building an arbitrarily large result.

    Every non-zero-width element consumes at least one input byte, so a
    legitimate count can never exceed the total input length.  Zero-width
    elements consume nothing and instead draw down a budget shared across the
    whole decode.
    """
    count = source.read_leb128()
    if zero_width:
        if count > limits.zero_width_budget:
            raise UnsupportedBinaryTypeError(f"{what} count {count} exceeds the zero-width element budget")
        limits.zero_width_budget -= count
    elif count > limits.input_len:
        raise UnsupportedBinaryTypeError(f"{what} count {count} exceeds bound {limits.input_len}")
    return count


def _read_self_describing(source: ByteSource, ctx: QueryContext, limits: _Limits) -> Any:
    """Read one ``<encoded type><value>`` pair, as written by SerializationDynamic."""
    node = _read_encoded_type(source)
    if node.kind == "nothing":
        # SerializationDynamic encodes a null value as the bare Nothing type
        return None
    return _read_binary_value(source, node, ctx, limits)


def _read_binary_value(source: ByteSource, node: _Node, ctx: QueryContext, limits: _Limits) -> Any:
    """Read one value in ISerialization::serializeBinary format."""
    kind = node.kind

    if kind == "scalar":
        ch_type = node.ch_type
        assert ch_type is not None
        read_state = ch_type.read_column_prefix(source, ctx)
        col_data = ch_type.read_column_data(source, 1, ctx, read_state)
        if not col_data:
            raise UnsupportedBinaryTypeError(f"{ch_type.name} read produced no value")
        return col_data[0]

    if kind == "nothing":
        # The server never serializes a Nothing value; the bare Nothing type
        # only appears as SerializationDynamic's encoding of null, handled in
        # _read_self_describing.  Reaching here means the buffer is corrupt.
        raise UnsupportedBinaryTypeError("Nothing values are not serializable")

    if kind == "array":
        child = _child_of(node)
        count = _read_element_count(source, child.zero_width, limits, "Array element")
        return [_read_binary_value(source, child, ctx, limits) for _ in range(count)]

    if kind == "nullable":
        if source.read_byte():
            return None
        return _read_binary_value(source, _child_of(node), ctx, limits)

    if kind == "map":
        # Serialized as the nested Array(Tuple(K, V))
        key_node, value_node = node.children
        zero_width = key_node.zero_width and value_node.zero_width
        count = _read_element_count(source, zero_width, limits, "Map entry")
        mapping = {}
        for _ in range(count):
            key = _read_binary_value(source, key_node, ctx, limits)
            mapping[key] = _read_binary_value(source, value_node, ctx, limits)
        return mapping

    if kind == "tuple":
        return tuple(_read_binary_value(source, child, ctx, limits) for child in node.children)

    if kind == "named_tuple":
        return {name: _read_binary_value(source, child, ctx, limits) for name, child in zip(node.names, node.children)}

    if kind == "dynamic":
        return _read_self_describing(source, ctx, limits)

    if kind == "json":
        from clickhouse_connect.datatypes.dynamic import _nest_value  # circular import

        typed_paths = node.typed_paths
        obj: dict[str, Any] = {}
        for _ in range(source.read_leb128()):
            path = source.read_leb128_str()
            path_node = typed_paths.get(path)
            if path_node is None:
                # dynamic path or shared data: value is self describing
                value = _read_self_describing(source, ctx, limits)
            else:
                value = _read_binary_value(source, path_node, ctx, limits)
            if value is not None:
                _nest_value(obj, path, value)
        return obj

    raise UnsupportedBinaryTypeError(f"Unhandled node kind {kind}")


def _decode_binary_value(binary_data: bytes, ctx: QueryContext) -> Any:
    """Decode ``<encoded type><serializeBinary value>`` into a Python object.

    :param binary_data: The complete encoded value, including the leading type encoding.
    :param ctx: Query context used for scalar column decoding.
    :returns: The decoded Python value.
    :raises UnsupportedBinaryTypeError: If the type encoding is not supported,
        or if the parser does not consume exactly ``binary_data``.
    :raises StreamCompleteException: If ``binary_data`` is truncated.
    """
    source = _byte_source(binary_data)
    node = _read_encoded_type(source)
    if node.kind == "nothing":
        # Shared data never stores nulls; a bare Nothing type is corrupt input
        raise UnsupportedBinaryTypeError("Nothing is not a serializable value type")
    value = _read_binary_value(source, node, ctx, _Limits(len(binary_data)))
    try:
        source.read_byte()
    except StreamCompleteException:
        return value
    raise UnsupportedBinaryTypeError("Trailing bytes after decode")
