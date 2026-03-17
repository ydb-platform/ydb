from dataclasses import dataclass
from typing import IO

from typing_extensions import Self

from pure_protobuf.exceptions import IncorrectWireTypeError
from pure_protobuf.helpers._dataclasses import KW_ONLY, SLOTS
from pure_protobuf.io.varint import read_unsigned_varint, write_unsigned_varint
from pure_protobuf.io.wire_type import WireType


@dataclass(frozen=True, **KW_ONLY, **SLOTS)
class Tag:
    """
    Represents a field tag: a pair of key and wire type.

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#structure.
    """

    field_number: int
    wire_type: WireType

    @classmethod
    def read_from(cls, io: IO[bytes]) -> Self:
        """Read a tag from the file object."""
        encoded_tag = read_unsigned_varint(io)
        encoded_wire_type = encoded_tag & 0b111
        try:
            wire_type = WireType(encoded_wire_type)
        except ValueError as e:
            raise IncorrectWireTypeError(f"incorrect wire type {encoded_wire_type}") from e
        return cls(field_number=(encoded_tag >> 3), wire_type=wire_type)

    def encode(self) -> int:
        """Convert the tag into its encoded format."""
        return (self.field_number << 3) | self.wire_type.value  # type: ignore

    def write_to(self, io: IO[bytes]) -> None:
        """Write the tag into the file object."""
        write_unsigned_varint(self.encode(), io)
