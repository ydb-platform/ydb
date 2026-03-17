"""Test suite for prance.util ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


import pytest

from prance import util


def test_stringify_keys():
    test = {
        "str": 42,
        "unicode": 123,
        "nested": {
            "str": 42,
            "unicode": 123,
        },
        42: "int",
        3.14: "float",
    }

    result = util.stringify_keys(test)

    def assert_object_identity(key, first, second):
        ret = None
        for k1, v1 in first.items():
            for k2, v2 in second.items():
                if k1 == key and k2 == key:
                    assert id(k1) == id(k2)
                    ret = id(k1)
        assert ret is not None
        return ret

    # The number of keys must not have changed
    assert len(test) == len(result)
    assert len(test["nested"]) == len(result["nested"])

    # str/unicode keys must remain absolutely unchanged. We're testing for
    # object identity.
    found = []
    for key in ("str", "unicode"):
        found.append(assert_object_identity(key, test, result))
        found.append(assert_object_identity(key, test["nested"], result["nested"]))

    # for all other keys we find in the test set, there must exist a
    # stringified version of it in the result set
    for key, value in test.items():
        if key in found:
            continue

        assert str(key) in result

    for key, value in test["nested"].items():
        if key in found:
            continue

        assert str(key) in result["nested"]
