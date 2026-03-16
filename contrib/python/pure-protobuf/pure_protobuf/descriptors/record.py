from __future__ import annotations

from dataclasses import dataclass
from enum import IntEnum
from types import GenericAlias
from typing import TYPE_CHECKING, Any, ClassVar, Generic
from urllib.parse import ParseResult

from typing_extensions import Self

from pure_protobuf._accumulators import AccumulateLastOneWins
from pure_protobuf._mergers import MergeLastOneWins
from pure_protobuf.annotations import ZigZagInt, double, fixed32, fixed64, sfixed32, sfixed64, uint
from pure_protobuf.exceptions import UnsupportedAnnotationError
from pure_protobuf.helpers._dataclasses import KW_ONLY, SLOTS
from pure_protobuf.helpers.itertools import ReadCallback
from pure_protobuf.interfaces._vars import RecordT
from pure_protobuf.interfaces.accumulate import Accumulate
from pure_protobuf.interfaces.merge import Merge
from pure_protobuf.interfaces.read import ReadTyped
from pure_protobuf.interfaces.write import Write
from pure_protobuf.io.bytes_ import read_bytes, read_string, write_bytes, write_string
from pure_protobuf.io.struct_ import ReadStruct, WriteStruct
from pure_protobuf.io.url import ReadUrl, WriteUrl
from pure_protobuf.io.varint import (
    ReadEnum,
    ReadTwosComplimentVarint,
    ReadZigZagVarint,
    WriteEnum,
    WriteTwosComplimentVarint,
    WriteZigZagVarint,
    read_bool,
    read_unsigned_varint,
    write_bool,
    write_unsigned_varint,
)
from pure_protobuf.io.wire_type import WireType
from pure_protobuf.io.wrappers import ReadMaybePacked, ReadStrictlyTyped

if TYPE_CHECKING:
    from pure_protobuf.message import BaseMessage


@dataclass(**KW_ONLY, **SLOTS)
class RecordDescriptor(Generic[RecordT]):
    """Describes how records should be read, written, accumulated, and merged. Not a «real» Python descriptor."""

    wire_type: WireType
    """Field's record [wire type](https://developers.google.com/protocol-buffers/docs/encoding#structure)."""

    write: Write[RecordT]
    """Write a complete value to the stream, altogether with its tag."""

    read: ReadTyped[RecordT]
    """
    Read a record from the stream.

    This behaves differently from the [`write`][pure_protobuf.descriptors.record.RecordDescriptor.write],
    because it's only supposed to read a single record from the stream
    (it may be, for example, just one item of a packed repeated field).
    Also, it assumes that the tag has already been read by [`BaseMessage`][base-message].
    """

    accumulate: Accumulate[RecordT, RecordT] = AccumulateLastOneWins()
    """
    Accumulate a value from the stream into an existing field value.
    It follows the `read` to decide which value should be assigned to the attribute.
    """

    merge: Merge[RecordT] = MergeLastOneWins()
    """Merge two values of the same field from different messages. Only called in a message merger."""

    __PREDEFINED__: ClassVar[dict[Any, RecordDescriptor]]
    """Pre-defined descriptors for primitive types."""

    @classmethod
    def _from_inner_type_hint(
        cls,
        message_type: type[BaseMessage],
        inner_hint: Any,
    ) -> RecordDescriptor[Any]:
        """
        Construct a descriptor from the inner type hint.

        Examples:
            - For `Annotated[int, Field[1]]` the inner hint is `int`.
            - For `Annotated[List[int], Field[1]]` it's also `int`
              since the `List` has already been extracted by `Field._from_annotated_args`.

        Args:
            message_type: message type which contains the attribute being described
            inner_hint: the attribute's own type hint
        """

        from pure_protobuf.message import BaseMessage

        try:
            singular = RecordDescriptor.__PREDEFINED__[inner_hint]
        except KeyError:
            pass
        else:
            return RecordDescriptor(
                wire_type=singular.wire_type,
                read=singular.read,
                write=singular.write,
            )

        if inner_hint is Self:
            # Support recursive types.
            return message_type._init_embedded_descriptor()

        if isinstance(inner_hint, type):
            if issubclass(inner_hint, IntEnum):
                return RecordDescriptor(
                    wire_type=WireType.VARINT,
                    write=WriteEnum[inner_hint](),
                    read=ReadMaybePacked[inner_hint](ReadEnum(inner_hint), WireType.VARINT),
                )
            if (
                not isinstance(inner_hint, GenericAlias)  # TODO: remove with Python 3.9 end-of-life.
                and issubclass(inner_hint, BaseMessage)
            ):
                return inner_hint._init_embedded_descriptor()

        raise UnsupportedAnnotationError(f"type annotation `{inner_hint!r}` is not supported")


BOOL_DESCRIPTOR: RecordDescriptor[bool] = RecordDescriptor(
    wire_type=WireType.VARINT,
    write=write_bool,
    read=ReadMaybePacked[bool](ReadCallback(read_bool), WireType.VARINT),
)
BYTES_DESCRIPTOR: RecordDescriptor[bytes] = RecordDescriptor(
    wire_type=WireType.LEN,
    write=write_bytes,
    read=ReadStrictlyTyped(ReadCallback(read_bytes), WireType.LEN),
)
FLOAT_DESCRIPTOR: RecordDescriptor[float] = RecordDescriptor(
    wire_type=WireType.I32,
    read=ReadMaybePacked(ReadStruct[float]("<f"), WireType.I32),
    write=WriteStruct[float]("<f"),
)
DOUBLE_DESCRIPTOR: RecordDescriptor[double] = RecordDescriptor(
    wire_type=WireType.I64,
    read=ReadMaybePacked(ReadStruct[double]("<d"), WireType.I64),
    write=WriteStruct[double]("<d"),
)
SIGNED_INT32_DESCRIPTOR: RecordDescriptor[sfixed32] = RecordDescriptor(
    wire_type=WireType.I32,
    read=ReadMaybePacked(ReadStruct[sfixed32]("<i"), WireType.I32),
    write=WriteStruct[sfixed32]("<i"),
)
UNSIGNED_INT32_DESCRIPTOR: RecordDescriptor[fixed32] = RecordDescriptor(
    wire_type=WireType.I32,
    read=ReadMaybePacked(ReadStruct[fixed32]("<I"), WireType.I32),
    write=WriteStruct[fixed32]("<I"),
)
SIGNED_INT64_DESCRIPTOR: RecordDescriptor[sfixed64] = RecordDescriptor(
    wire_type=WireType.I64,
    read=ReadMaybePacked(ReadStruct[sfixed64]("<i"), WireType.I64),
    write=WriteStruct[sfixed64]("<q"),
)
UNSIGNED_INT64_DESCRIPTOR: RecordDescriptor[fixed64] = RecordDescriptor(
    wire_type=WireType.I64,
    read=ReadMaybePacked(ReadStruct[fixed64]("<I"), WireType.I64),
    write=WriteStruct[fixed64]("<Q"),
)
URL_DESCRIPTOR: RecordDescriptor[ParseResult] = RecordDescriptor(
    wire_type=WireType.LEN,
    read=ReadStrictlyTyped[ParseResult](ReadUrl(), WireType.LEN),
    write=WriteUrl(),
)

RecordDescriptor.__PREDEFINED__ = {
    bool: BOOL_DESCRIPTOR,
    bytes: BYTES_DESCRIPTOR,
    bytearray: BYTES_DESCRIPTOR,
    fixed32: UNSIGNED_INT32_DESCRIPTOR,
    fixed64: UNSIGNED_INT64_DESCRIPTOR,
    float: FLOAT_DESCRIPTOR,
    double: DOUBLE_DESCRIPTOR,
    int: RecordDescriptor(
        wire_type=WireType.VARINT,
        write=WriteTwosComplimentVarint(),
        read=ReadMaybePacked[int](ReadCallback(ReadTwosComplimentVarint()), WireType.VARINT),
    ),
    memoryview: BYTES_DESCRIPTOR,
    ParseResult: URL_DESCRIPTOR,
    sfixed32: SIGNED_INT32_DESCRIPTOR,
    sfixed64: UNSIGNED_INT64_DESCRIPTOR,
    str: RecordDescriptor(
        wire_type=WireType.LEN,
        write=write_string,
        read=ReadStrictlyTyped(ReadCallback(read_string), WireType.LEN),
    ),
    uint: RecordDescriptor(
        wire_type=WireType.VARINT,
        write=write_unsigned_varint,
        read=ReadMaybePacked[int](ReadCallback(read_unsigned_varint), WireType.VARINT),
    ),
    ZigZagInt: RecordDescriptor(
        wire_type=WireType.VARINT,
        write=WriteZigZagVarint(),
        read=ReadMaybePacked[int](ReadCallback(ReadZigZagVarint()), WireType.VARINT),
    ),
}

try:
    from collections.abc import ByteString
except ImportError:
    pass
else:
    RecordDescriptor.__PREDEFINED__[ByteString] = BYTES_DESCRIPTOR
