from __future__ import annotations

from abc import ABC
from collections.abc import Mapping
from io import BytesIO
from typing import IO, Any, ClassVar

from typing_extensions import Self

try:
    from inspect import get_annotations  # type: ignore[attr-defined]
except ImportError:
    from get_annotations import get_annotations  # type: ignore[no-redef]

from pure_protobuf._accumulators import AccumulateMessages
from pure_protobuf._mergers import MergeMessages
from pure_protobuf.descriptors._field import _FieldDescriptor
from pure_protobuf.descriptors.record import RecordDescriptor
from pure_protobuf.helpers.itertools import ReadCallback
from pure_protobuf.interfaces._skip import Skip, skip_no_operation
from pure_protobuf.io.bytes_ import skip_bytes
from pure_protobuf.io.fixed32 import skip_fixed_32
from pure_protobuf.io.fixed64 import skip_fixed_64
from pure_protobuf.io.tag import Tag
from pure_protobuf.io.varint import skip_varint
from pure_protobuf.io.wire_type import WireType
from pure_protobuf.io.wrappers import (
    ReadLengthDelimited,
    ReadStrictlyTyped,
    WriteLengthDelimited,
    to_bytes,
)


class BaseMessage(ABC):
    """Base message class, inherit from it to define a specific message."""

    __slots__ = ()

    __PROTOBUF_FIELDS_BY_NUMBER__: ClassVar[dict[int, tuple[str, _FieldDescriptor]]]
    __PROTOBUF_FIELDS_BY_NAME__: ClassVar[dict[str, _FieldDescriptor]]

    __PROTOBUF_SKIP__: ClassVar[Mapping[WireType, Skip]] = {
        WireType.VARINT: skip_varint,
        WireType.I64: skip_fixed_64,
        WireType.LEN: skip_bytes,
        WireType.I32: skip_fixed_32,
        WireType.SGROUP: skip_no_operation,
        WireType.EGROUP: skip_no_operation,
    }
    """Defines how to skip a field of the given wire type."""

    def __init_subclass__(cls) -> None:  # noqa: D105
        cls.__PROTOBUF_FIELDS_BY_NUMBER__ = {}
        cls.__PROTOBUF_FIELDS_BY_NAME__ = {}

        type_hints: dict[str, Any] = get_annotations(cls, eval_str=True)
        for name, hint in type_hints.items():
            descriptor = _FieldDescriptor.from_attribute(cls, hint)
            if descriptor is not None:
                cls.__PROTOBUF_FIELDS_BY_NUMBER__[descriptor.number] = (name, descriptor)
                cls.__PROTOBUF_FIELDS_BY_NAME__[name] = descriptor
                one_of = descriptor.one_of
                if one_of is not None:
                    one_of._add_field(descriptor.number, name)

    @classmethod
    def read_from(cls, io: IO[bytes]) -> Self:
        """Read a message from the file."""

        values: dict[str, Any] = {}
        while True:
            try:
                tag = Tag.read_from(io)
            except EOFError:
                break
            try:
                name, descriptor = cls.__PROTOBUF_FIELDS_BY_NUMBER__[tag.field_number]
            except KeyError:
                # The field is not defined, skip it in the stream.
                # TODO: https://github.com/eigenein/protobuf/issues/99.
                cls.__PROTOBUF_SKIP__[tag.wire_type](io)
            else:
                # Read the value and accumulate it.
                values[name] = descriptor.accumulate(
                    values.get(name),
                    descriptor.read(io, tag.wire_type),
                )

                # Possibly update the one-of field.
                one_of = descriptor.one_of
                if one_of is not None:
                    one_of._keep_values(values, descriptor.number)

        return cls(**values)

    @classmethod
    def loads(cls, buffer: bytes) -> Self:
        """
        Read a message from the buffer.

        This is functionally the same as calling `read_from(BytesIO(buffer))`.
        """
        return cls.read_from(BytesIO(buffer))

    def write_to(self, io: IO[bytes]) -> None:
        """Write the message to the file."""
        for _, (name, descriptor) in self.__PROTOBUF_FIELDS_BY_NUMBER__.items():
            descriptor.write(getattr(self, name), io)

    def __bytes__(self) -> bytes:
        """
        Convert the message to a bytestring.

        This is functionally the same as calling `dumps()` or `write_to(BytesIO(…))`.
        """
        return to_bytes(BaseMessage.write_to, self)

    def dumps(self) -> bytes:
        """
        Convert the message to a bytestring.

        This is functionally the same as calling `bytes(message)` or `write_to(BytesIO(…))`.
        """
        return bytes(self)

    def __setattr__(self, name: str, value: Any) -> None:  # noqa: D105
        super().__setattr__(name, value)
        descriptor = self.__PROTOBUF_FIELDS_BY_NAME__[name]
        if (one_of := descriptor.one_of) is not None and value is not None:
            one_of._keep_attribute(self, descriptor.number)

    @classmethod
    def _init_embedded_descriptor(cls) -> RecordDescriptor[Self]:
        accumulate = AccumulateMessages(cls)
        return RecordDescriptor(
            wire_type=WireType.LEN,
            write=WriteLengthDelimited(cls.write_to),
            read=ReadStrictlyTyped(ReadLengthDelimited(ReadCallback(cls.read_from)), WireType.LEN),
            accumulate=accumulate,
            merge=MergeMessages(accumulate),
        )
