"""Field type definitions."""

from enum import IntEnum
from typing import Dict, List, Tuple


class FieldType(IntEnum):
    """Field types mapped to their integer representation."""

    PROPRIETARY = 0
    BYTE = 1
    ASCII = 2
    SHORT = 3
    LONG = 4
    RATIO = 5
    SIGNED_BYTE = 6
    UNDEFINED = 7
    SIGNED_SHORT = 8
    SIGNED_LONG = 9
    SIGNED_RATIO = 10
    FLOAT_32 = 11
    FLOAT_64 = 12
    IFD = 13


SIGNED_FIELD_TYPES: List[FieldType] = [
    FieldType.SIGNED_BYTE,
    FieldType.SIGNED_SHORT,
    FieldType.SIGNED_LONG,
    FieldType.SIGNED_RATIO,
]

RATIO_FIELD_TYPES: List[FieldType] = [FieldType.RATIO, FieldType.SIGNED_RATIO]

FLOAT_FIELD_TYPES: List[FieldType] = [FieldType.FLOAT_32, FieldType.FLOAT_64]

# Field type descriptions as (length, full name) tuples
FIELD_DEFINITIONS: Dict[FieldType, Tuple[int, str]] = {
    FieldType.PROPRIETARY: (0, "Proprietary"),
    FieldType.BYTE: (1, "Byte"),
    FieldType.ASCII: (1, "ASCII"),
    FieldType.SHORT: (2, "Short"),
    FieldType.LONG: (4, "Long"),
    FieldType.RATIO: (8, "Ratio"),
    FieldType.SIGNED_BYTE: (1, "Signed Byte"),
    FieldType.UNDEFINED: (1, "Undefined"),
    FieldType.SIGNED_SHORT: (2, "Signed Short"),
    FieldType.SIGNED_LONG: (4, "Signed Long"),
    FieldType.SIGNED_RATIO: (8, "Signed Ratio"),
    FieldType.FLOAT_32: (4, "Single-Precision Floating Point (32-bit)"),
    FieldType.FLOAT_64: (8, "Double-Precision Floating Point (64-bit)"),
    FieldType.IFD: (4, "IFD"),
}
