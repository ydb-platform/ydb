from . import (
    assert_that,
    all_of,
    anything as _anything,
    contains_exactly as contains_inanyorder,
    equal_to,
    has_attr as has_property,
    has_attrs as has_properties,
    is_sequence as contains,
)


__all__ = [
    "assert_that",
    "all_of",
    "anything",
    "contains_inanyorder",
    "equal_to",
    "has_property",
    "has_properties",
    "contains",
]

def anything():
    return _anything
