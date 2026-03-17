from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Annotated, Any, Generic, Optional, cast

from typing_extensions import get_args as get_type_args
from typing_extensions import get_origin as get_type_origin

from pure_protobuf._accumulators import AccumulateAppend
from pure_protobuf._mergers import MergeConcatenate
from pure_protobuf.annotations import Field
from pure_protobuf.descriptors.record import RecordDescriptor
from pure_protobuf.helpers._dataclasses import KW_ONLY, SLOTS
from pure_protobuf.helpers._typing import extract_optional, extract_repeated
from pure_protobuf.interfaces._vars import FieldT, RecordT
from pure_protobuf.interfaces.accumulate import Accumulate
from pure_protobuf.interfaces.merge import Merge
from pure_protobuf.interfaces.read import ReadTyped
from pure_protobuf.interfaces.write import Write
from pure_protobuf.io.tag import Tag
from pure_protobuf.io.wire_type import WireType
from pure_protobuf.io.wrappers import (
    WriteLengthDelimited,
    WriteOptional,
    WriteRepeated,
    WriteTagged,
)

if TYPE_CHECKING:
    from pure_protobuf.annotations import OneOf
    from pure_protobuf.message import BaseMessage


@dataclass(frozen=True, **SLOTS, **KW_ONLY)
class _FieldDescriptor(Generic[FieldT, RecordT]):
    """
    Describes how the field should be read and written.

    Notes:
        - It's not a «real» Python descriptor, just didn't come up with a better name.
        - It gets converted from `RecordDescriptor`.
    """

    number: int
    """
    Field's number.

    See Also:
        - https://developers.google.com/protocol-buffers/docs/proto3#assigning_field_numbers
    """

    one_of: Optional[OneOf]
    """
    See Also:
        - https://developers.google.com/protocol-buffers/docs/proto3#oneof
    """

    write: Write[FieldT]
    read: ReadTyped[RecordT]
    accumulate: Accumulate[FieldT, RecordT]
    merge: Merge[FieldT]

    @classmethod
    def from_attribute(
        cls,
        message_type: type[BaseMessage],
        attribute_hint: Any,
    ) -> Optional[_FieldDescriptor[Any, Any]]:
        """
        Construct a descriptor from the attribute's type hint.

        Returns:
            Field's descriptor, or `None` if skipped.
        """
        if get_type_origin(attribute_hint) is not Annotated:
            # It's not an annotated attribute, so ignore it.
            return None

        inner_hint, *annotated_args = get_type_args(attribute_hint)

        # noinspection PyProtectedMember
        field = Field._from_annotated_args(*annotated_args)
        if field is None:
            # `Field` annotation is missing, ignore the attribute.
            return None

        # Extract the flags from the hint.
        inner_hint, _ = extract_optional(inner_hint)
        inner_hint, is_repeated = extract_repeated(inner_hint)

        inner: RecordDescriptor[RecordT] = RecordDescriptor._from_inner_type_hint(
            message_type,
            inner_hint,
        )
        write = cast(Write[FieldT], inner.write)
        accumulate = cast(Accumulate[FieldT, RecordT], inner.accumulate)
        merge = cast(Merge[FieldT], inner.merge)

        # Abandon hope all ye who enter here.
        if field._packed_or(is_repeated and inner.wire_type.is_primitive_numeric):
            # Repeated fields of primitive numeric types are packed by default per the specification.
            if is_repeated:
                # Repeated packed field are untagged internally.
                write = WriteRepeated(write)

            # Now we can wrap it into a length-delimited value.
            write = WriteLengthDelimited(write)

            # And finally, tag it. The wire type is overridden.
            tag = Tag(field_number=field.number, wire_type=WireType.LEN)
            write = WriteTagged(write, tag)
        else:
            # In unpacked fields each value is separately tagged, thus applied first.
            tag = Tag(field_number=field.number, wire_type=inner.wire_type)
            write = WriteTagged(write, tag)
            if is_repeated:
                # `read` still reads one record at a time.
                write = WriteRepeated(write)

        if is_repeated:
            # Merger should concatenate repeated fields.
            merge = cast(Merge[FieldT], MergeConcatenate[RecordT]())
            accumulate = cast(Accumulate[FieldT, RecordT], AccumulateAppend[RecordT]())

        # And now just build and return the final descriptor.
        return cls(
            number=field.number,
            one_of=field.one_of,
            write=WriteOptional(write),
            read=inner.read,
            accumulate=accumulate,
            merge=merge,
        )
