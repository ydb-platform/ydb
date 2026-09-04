from typing import Dict

import pytest

from deepmerge.strategy.type_conflict import TypeConflictStrategies

EMPTY_DICT: Dict = {}

CONTENT_AS_LIST = [{"key": "val"}]

BASE = "base"


def test_merge_if_not_empty():
    strategy = TypeConflictStrategies.strategy_override_if_not_empty(
        {}, [], EMPTY_DICT, CONTENT_AS_LIST
    )
    assert strategy == CONTENT_AS_LIST

    strategy = TypeConflictStrategies.strategy_override_if_not_empty(
        {}, [], CONTENT_AS_LIST, EMPTY_DICT
    )
    assert strategy == CONTENT_AS_LIST

    strategy = TypeConflictStrategies.strategy_override_if_not_empty({}, [], CONTENT_AS_LIST, None)
    assert strategy == CONTENT_AS_LIST


@pytest.mark.parametrize(
    "nxt, expected",
    [
        (0, 0),
        (False, False),
        (0.0, 0.0),
        (0j, 0j),
        ("0", "0"),
        ([0], [0]),
        (None, BASE),
        ("", BASE),
        ([], BASE),
        ({}, BASE),
        (set(), BASE),
        ((), BASE),
    ],
    ids=[
        "zero int",
        "false",
        "zero float",
        "zero complex",
        "zero string",
        "list holding zero",
        "none",
        "empty string",
        "empty list",
        "empty dict",
        "empty set",
        "empty tuple",
    ],
)
def test_merge_if_not_empty_falsy(nxt, expected):
    """Only null and empty sized values keep base; falsy primitives override it."""
    strategy = TypeConflictStrategies.strategy_override_if_not_empty({}, [], BASE, nxt)
    assert strategy == expected
    # 0 == False, so compare types too
    assert type(strategy) is type(expected)
