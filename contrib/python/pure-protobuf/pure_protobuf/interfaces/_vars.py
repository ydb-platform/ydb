from typing import TYPE_CHECKING, TypeVar

if TYPE_CHECKING:
    from pure_protobuf.message import BaseMessage  # noqa

FieldT = TypeVar("FieldT")
"""Message field's type."""

FieldT_contra = TypeVar("FieldT_contra", contravariant=True)

RecordT = TypeVar("RecordT")
"""Field's record type. May be the field's type itself, or a single item out of it."""

RecordT_contra = TypeVar("RecordT_contra", contravariant=True)
RecordT_co = TypeVar("RecordT_co", covariant=True)

MessageT = TypeVar("MessageT", bound="BaseMessage")
