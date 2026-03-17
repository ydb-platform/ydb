"""
A right-favoring Mapping merge.
"""
from collections import Mapping


__all__ = ['merge']


def merge(left, right):
    """
    Merge two mappings objects together, combining overlapping Mappings,
    and favoring right-values

    left: The left Mapping object.
    right: The right (favored) Mapping object.

    NOTE: This is not commutative (merge(a,b) != merge(b,a)).
    """
    merged = {}

    left_keys = frozenset(left)
    right_keys = frozenset(right)

    # Items only in the left Mapping
    for key in left_keys - right_keys:
        merged[key] = left[key]

    # Items only in the right Mapping
    for key in right_keys - left_keys:
        merged[key] = right[key]

    # in both
    for key in left_keys & right_keys:
        left_value = left[key]
        right_value = right[key]

        if (isinstance(left_value, Mapping) and
                isinstance(right_value, Mapping)):  # recursive merge
            merged[key] = merge(left_value, right_value)
        else:  # overwrite with right value
            merged[key] = right_value

    return merged
