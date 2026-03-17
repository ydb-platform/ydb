"""
Unit of a dictionary
"""

PRECISION_MASK = 0xFFFFFFFF

OFFSET_MAX = 1 << 21
IS_LEAF_BIT = 1 << 31
HAS_LEAF_BIT = 1 << 8
EXTENSION_BIT = 1 << 9


def has_leaf(base: int, _mask: int = HAS_LEAF_BIT) -> bool:
    """Check if a unit has a leaf as a child or not."""
    return bool(base & _mask)


def value(base: int, _mask: int = ~IS_LEAF_BIT & PRECISION_MASK) -> int:
    """Check if a unit corresponds to a leaf or not."""
    return base & _mask


def label(base: int, _mask: int = IS_LEAF_BIT | 0xFF) -> int:
    """Read a label with a leaf flag from a non-leaf unit."""
    return base & _mask


def offset(base: int) -> int:
    """Read an offset to child units from a non-leaf unit."""
    return ((base >> 10) << ((base & EXTENSION_BIT) >> 6)) & PRECISION_MASK
