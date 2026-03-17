# Copyright (c) 2014-2022, Manfred Moitzi
# License: MIT License
"""
DXF Types
=========

Required DXF tag interface:

    - property :attr:`code`: group code as int
    - property :attr:`value`: tag value of unspecific type
    - :meth:`dxfstr`: returns the DXF string
    - :meth:`clone`: returns a deep copy of tag

"""
from __future__ import annotations
from typing import (
    Union,
    Iterable,
    Sequence,
    Type,
    Any,
)
from array import array
from itertools import chain
from binascii import unhexlify, hexlify
import reprlib
from ezdxf.math import Vec3


TAG_STRING_FORMAT = "%3d\n%s\n"
POINT_CODES = {
    10,
    11,
    12,
    13,
    14,
    15,
    16,
    17,
    18,
    110,
    111,
    112,
    210,
    211,
    212,
    213,
    1010,
    1011,
    1012,
    1013,
}

MAX_GROUP_CODE = 1071
GENERAL_MARKER = 0
SUBCLASS_MARKER = 100
XDATA_MARKER = 1001
EMBEDDED_OBJ_MARKER = 101
APP_DATA_MARKER = 102
EXT_DATA_MARKER = 1001
GROUP_MARKERS = {
    GENERAL_MARKER,
    SUBCLASS_MARKER,
    EMBEDDED_OBJ_MARKER,
    APP_DATA_MARKER,
    EXT_DATA_MARKER,
}
BINARY_FLAGS = {70, 90}
HANDLE_CODES = {5, 105}
POINTER_CODES = set(chain(range(320, 370), range(390, 400), (480, 481, 1005)))

# pointer group codes 320-329 are not translated during INSERT and XREF operations
TRANSLATABLE_POINTER_CODES = set(
    chain(range(330, 370), range(390, 400), (480, 481, 1005))
)
HEX_HANDLE_CODES = set(chain(HANDLE_CODES, POINTER_CODES))
BINARY_DATA = {310, 311, 312, 313, 314, 315, 316, 317, 318, 319, 1004}
EMBEDDED_OBJ_STR = "Embedded Object"

BYTES = set(range(290, 300))  # bool

INT16 = set(
    chain(
        range(60, 80),
        range(170, 180),
        range(270, 290),
        range(370, 390),
        range(400, 410),
        range(1060, 1071),
    )
)

INT32 = set(
    chain(
        range(90, 100),
        range(420, 430),
        range(440, 450),
        range(450, 460),  # Long in DXF reference, ->signed<- or unsigned?
        [1071],
    )
)

INT64 = set(range(160, 170))

DOUBLE = set(
    chain(
        range(10, 60),
        range(110, 150),
        range(210, 240),
        range(460, 470),
        range(1010, 1060),
    )
)

VALID_XDATA_GROUP_CODES = {
    1000,
    1001,
    1002,
    1003,
    1004,
    1005,
    1010,
    1011,
    1012,
    1013,
    1040,
    1041,
    1042,
    1070,
    1071,
}


def _build_type_table(types):
    table = {}
    for caster, codes in types:
        for code in codes:
            table[code] = caster
    return table


TYPE_TABLE = _build_type_table(
    [
        # all group code < 0 are spacial tags for internal use
        (float, DOUBLE),
        (int, BYTES),
        (int, INT16),
        (int, INT32),
        (int, INT64),
    ]
)


class DXFTag:
    """Immutable DXFTag class.

    Args:
        code: group code as int
        value: tag value, type depends on group code

    """

    __slots__ = ("_code", "_value")

    def __init__(self, code: int, value: Any):
        self._code: int = int(code)
        # Do not use _value, always use property value - overwritten in subclasses
        self._value = value

    def __str__(self) -> str:
        """Returns content string ``'(code, value)'``."""
        return str((self._code, self.value))

    def __repr__(self) -> str:
        """Returns representation string ``'DXFTag(code, value)'``."""
        return f"DXFTag{str(self)}"

    @property
    def code(self) -> int:
        return self._code

    @property
    def value(self) -> Any:
        return self._value

    def __getitem__(self, index: int):
        """Returns :attr:`code` for index 0 and :attr:`value` for index 1,
        emulates a tuple.
        """
        return (self._code, self.value)[index]

    def __iter__(self):
        """Returns (code, value) tuples."""
        yield self._code
        yield self.value

    def __eq__(self, other) -> bool:
        """``True`` if `other` and `self` has same content for :attr:`code`
        and :attr:`value`.
        """
        return (self._code, self.value) == other

    def __hash__(self):
        """Hash support, :class:`DXFTag` can be used in sets and as dict key."""
        return hash((self._code, self._value))

    def dxfstr(self) -> str:
        """Returns the DXF string e.g. ``'  0\\nLINE\\n'``"""
        return TAG_STRING_FORMAT % (self.code, self._value)

    def clone(self) -> "DXFTag":
        """Returns a clone of itself, this method is necessary for the more
        complex (and not immutable) DXF tag types.
        """
        return self  # immutable tags


# Special marker tag
NONE_TAG = DXFTag(0, 0)


def uniform_appid(appid: str) -> str:
    if appid[0] == "{":
        return appid
    else:
        return "{" + appid


def is_app_data_marker(tag: DXFTag) -> bool:
    return tag.code == APP_DATA_MARKER and tag.value.startswith("{")


def is_embedded_object_marker(tag: DXFTag) -> bool:
    return tag.code == EMBEDDED_OBJ_MARKER and tag.value == EMBEDDED_OBJ_STR


def is_arbitrary_pointer(tag: DXFTag) -> bool:
    """Arbitrary object handles; handle values that are taken "as is".
    They are not translated during INSERT and XREF operations.
    """
    return 319 < tag.code < 330


def is_soft_pointer(tag: DXFTag) -> bool:
    """Soft-pointer handle; arbitrary soft pointers to other objects within same DXF
    file or drawing. Translated during INSERT and XREF operations.
    """
    return 329 < tag.code < 340 or tag.code == 1005


def is_hard_pointer(tag: DXFTag) -> bool:
    """Hard-pointer handle; arbitrary hard pointers to other objects within same DXF
    file or drawing. Translated during INSERT and XREF operations. Hard pointers
    protect an object from being purged.
    """
    code = tag.code
    return 339 < code < 350 or 389 < code < 400 or 479 < code < 482


def is_soft_owner(tag: DXFTag) -> bool:
    """Soft-owner handle; arbitrary soft ownership links to other objects within same
    DXF file or drawing. Translated during INSERT and XREF operations.
    """
    return 349 < tag.code < 360


def is_hard_owner(tag: DXFTag) -> bool:
    """Hard-owner handle; arbitrary hard ownership links to other objects within same
    DXF file or drawing. Translated during INSERT and XREF operations. Hard owner handle
    protect an object from being purged.
    """
    return 359 < tag.code < 370


def is_translatable_pointer(tag: DXFTag) -> bool:
    # pointer group codes 320-329 are not translated during INSERT and XREF operations
    return tag.code in TRANSLATABLE_POINTER_CODES


class DXFVertex(DXFTag):
    """Represents a 2D or 3D vertex, stores only the group code of the
    x-component of the vertex, because the y-group-code is x-group-code + 10
    and z-group-code id x-group-code+20, this is a rule that ALWAYS applies.
    This tag is `immutable` by design, not by implementation.

    Args:
        code: group code of x-component
        value: sequence of x, y and optional z values

    """

    __slots__ = ()

    def __init__(self, code: int, value: Iterable[float]):
        super(DXFVertex, self).__init__(code, array("d", value))

    def __str__(self) -> str:
        return str(self.value)

    def __repr__(self) -> str:
        return f"DXFVertex({self.code}, {str(self)})"

    def __hash__(self):
        x, y, *z = self._value
        z = 0.0 if len(z) == 0 else z[0]
        return hash((self.code, x, y, z))

    @property
    def value(self) -> tuple[float, ...]:
        return tuple(self._value)

    def dxftags(self) -> Iterable[DXFTag]:
        """Returns all vertex components as single :class:`DXFTag` objects."""
        c = self.code
        return (
            DXFTag(code, value) for code, value in zip((c, c + 10, c + 20), self.value)
        )

    def dxfstr(self) -> str:
        """Returns the DXF string for all vertex components."""
        return "".join(tag.dxfstr() for tag in self.dxftags())


class DXFBinaryTag(DXFTag):
    """Immutable BinaryTags class - immutable by design, not by implementation."""

    __slots__ = ()

    def __str__(self) -> str:
        return f"({self.code}, {self.tostring()})"

    def __repr__(self) -> str:
        return f"DXFBinaryTag({self.code}, {reprlib.repr(self.tostring())})"

    def tostring(self) -> str:
        """Returns binary value as single hex-string."""
        assert isinstance(self.value, bytes)
        return hexlify(self.value).upper().decode()

    def dxfstr(self) -> str:
        """Returns the DXF string for all vertex components."""
        return TAG_STRING_FORMAT % (self.code, self.tostring())

    @classmethod
    def from_string(cls, code: int, value: Union[str, bytes]):
        return cls(code, unhexlify(value))


def dxftag(code: int, value: Any) -> DXFTag:
    """DXF tag factory function.

    Args:
        code: group code
        value: tag value

    Returns: :class:`DXFTag` or inherited

    """
    if code in BINARY_DATA:
        return DXFBinaryTag(code, value)
    elif code in POINT_CODES:
        return DXFVertex(code, value)
    else:
        return DXFTag(code, cast_tag_value(code, value))


def tuples_to_tags(iterable: Iterable[tuple[int, Any]]) -> Iterable[DXFTag]:
    """Returns an iterable if :class:`DXFTag` or inherited, accepts an
    iterable of (code, value) tuples as input.
    """
    for code, value in iterable:
        if code in POINT_CODES:
            yield DXFVertex(code, value)
        elif code in BINARY_DATA:
            assert isinstance(value, (str, bytes))
            yield DXFBinaryTag.from_string(code, value)
        else:
            yield DXFTag(code, value)


def is_valid_handle(handle) -> bool:
    if isinstance(handle, str):
        try:
            int(handle, 16)
            return True
        except (ValueError, TypeError):
            pass
    return False


def is_binary_data(code: int) -> bool:
    return code in BINARY_DATA


def is_pointer_code(code: int) -> bool:
    return code in POINTER_CODES


def is_point_code(code: int) -> bool:
    return code in POINT_CODES


def is_point_tag(tag: Sequence) -> bool:
    return tag[0] in POINT_CODES


def cast_tag_value(code: int, value: Any) -> Any:
    return TYPE_TABLE.get(code, str)(value)


def tag_type(code: int) -> Type:
    return TYPE_TABLE.get(code, str)


def strtag(tag: Union[DXFTag, tuple[int, Any]]) -> str:
    return TAG_STRING_FORMAT % tuple(tag)


def get_xcode_for(code) -> int:
    if code in HEX_HANDLE_CODES:
        return 1005
    if code in BINARY_DATA:
        return 1004
    type_ = TYPE_TABLE.get(code, str)
    if type_ is int:
        return 1070
    if type_ is float:
        return 1040
    return 1000


def cast_value(code: int, value):
    if value is not None:
        if code in POINT_CODES:
            return Vec3(value)
        return TYPE_TABLE.get(code, str)(value)
    else:
        return None


TAG_TYPES = {
    int: "<int>",
    float: "<float>",
    str: "<str>",
}


def tag_type_str(code: int) -> str:
    if code in GROUP_MARKERS:
        return "<ctrl>"
    elif code in HANDLE_CODES:
        return "<handle>"
    elif code in POINTER_CODES:
        return "<ref>"
    elif is_point_code(code):
        return "<point>"
    elif is_binary_data(code):
        return "<bin>"
    else:
        return TAG_TYPES[tag_type(code)]


def render_tag(tag: DXFTag, col: int) -> Any:
    code, value = tag
    if col == 0:
        return str(code)
    elif col == 1:
        return tag_type_str(code)
    elif col == 2:
        return str(value)
    else:
        raise IndexError(col)
