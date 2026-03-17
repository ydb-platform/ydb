from enum import IntEnum


class WireType(IntEnum):
    """
    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#structure.
    """

    VARINT = 0
    """
    Base 128 Varints: `int32`, `int64`, `uint32`, `uint64`, `sint32`, `sint64`, `bool`, `enum`.

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#varints.
    """

    I64 = 1
    """Fixed 64-bit value: `fixed64`, `sfixed64`, `double`."""

    LEN = 2
    """String, bytes, embedded messages, packed repeated fields."""

    SGROUP = 3
    """
    Group start (deprecated).

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#groups
    """

    EGROUP = 4
    """
    Group end (deprecated).

    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#groups
    """

    I32 = 5
    """Fixed 32-bit value: `fixed32`, `sfixed32`, `float`."""

    __PRIMITIVE_NUMERIC_TYPES__ = {VARINT, I32, I64}
    """
    See Also:
        - https://developers.google.com/protocol-buffers/docs/encoding#packed
    """

    @property
    def is_primitive_numeric(self) -> bool:
        return self in self.__PRIMITIVE_NUMERIC_TYPES__
