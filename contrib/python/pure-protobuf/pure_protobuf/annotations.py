"""Field annotations that may be used inside `Annotated`."""

from __future__ import annotations

from dataclasses import dataclass
from sys import version_info
from typing import TYPE_CHECKING, Any, ClassVar, NewType, Optional, Union

from pure_protobuf.exceptions import IncorrectAnnotationError
from pure_protobuf.helpers._dataclasses import SLOTS
from pure_protobuf.helpers._typing import DEFAULT, Sentinel

if version_info >= (3, 10):
    from dataclasses import KW_ONLY  # type: ignore[attr-defined]

if TYPE_CHECKING:
    from pure_protobuf.one_of import OneOf


@dataclass(frozen=True, **SLOTS)
class Field:
    """Annotates a Protocol Buffers field."""

    number: int
    """
    Specifies the field's number.

    See also:
        - https://developers.google.com/protocol-buffers/docs/proto3#assigning_field_numbers
    """

    if version_info >= (3, 10):
        _: ClassVar[KW_ONLY]

    packed: Union[bool, Sentinel] = DEFAULT
    """Specifies whether the field should be packed in its serialized representation."""

    one_of: Optional[OneOf] = None
    """Specifies a one-of group for this field."""

    @classmethod
    def _from_annotated_args(cls, *args: Any) -> Optional[Field]:
        """Extract itself from the `Annotated[_, *args]` type hint, if present."""
        for arg in args:
            if isinstance(arg, Field):
                arg._validate()
                return arg
        return None

    def _validate(self) -> None:
        if not 1 <= self.number <= 536_870_911:
            raise IncorrectAnnotationError(
                f"field number {self.number} is outside the allowed range",
            )
        if 19000 <= self.number <= 19999:
            raise IncorrectAnnotationError(f"field number {self.number} is reserved")

    def _packed_or(self, default: bool) -> bool:
        return self.packed if isinstance(self.packed, bool) else default


double = NewType("double", float)
"""
Fixed 64-bit floating point.

See Also:
    - https://developers.google.com/protocol-buffers/docs/proto3#scalar
    - https://developers.google.com/protocol-buffers/docs/encoding#non-varint-nums
"""

fixed32 = NewType("fixed32", int)
"""
Fixed unsigned 32-bit integer.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#non-varint-nums
"""

fixed64 = NewType("fixed64", int)
"""
Fixed unsigned 64-bit integer.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#non-varint-nums
"""

sfixed32 = NewType("sfixed32", int)
"""
Fixed signed 32-bit integer.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#non-varint-nums
"""

sfixed64 = NewType("sfixed64", int)
"""
Fixed signed 64-bit integer.

See Also:
    - https://developers.google.com/protocol-buffers/docs/proto3#scalar
    - https://developers.google.com/protocol-buffers/docs/encoding#non-varint-nums
"""

uint = NewType("uint", int)
"""
Unsigned variable-length integer.
"""

ZigZagInt = NewType("ZigZagInt", int)
"""
ZigZag-encoded variable-length integer.

See Also:
    - https://developers.google.com/protocol-buffers/docs/encoding#signed-ints
"""
