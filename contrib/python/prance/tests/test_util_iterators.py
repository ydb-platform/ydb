"""Test suite for prance.util.iterators ."""

__author__ = "Jens Finkhaeuser"
__copyright__ = "Copyright (c) 2016-2021 Jens Finkhaeuser"
__license__ = "MIT"
__all__ = ()


from prance.util import iterators


def test_item_iterator():
    tester = {
        "foo": 42,
        "bar": {
            "some": "dict",
        },
        "baz": [
            {1: 2},
            {3: 4},
        ],
    }

    frozen = tuple(iterators.item_iterator(tester))
    assert len(frozen) == 9, "Iterator did not yield the correct number of items!"

    # Test for a few paths
    assert ((), tester) in frozen
    assert (("bar", "some"), "dict") in frozen
    assert (("baz", 0, 1), 2) in frozen
    assert (("baz", 1), {3: 4}) in frozen


def test_item_iterator_empty():
    frozen = tuple(iterators.item_iterator({}))
    assert len(frozen) == 1, "Must return at least the item itself!"
    assert ((), {}) in frozen

    frozen = tuple(iterators.item_iterator([]))
    assert len(frozen) == 1, "Must return at least the item itself!"
    assert ((), []) in frozen


def test_reference_iterator_empty_dict():
    tester = {}
    frozen = tuple(iterators.reference_iterator(tester))
    assert len(frozen) == 0, "Found items when it should not have!"


def test_reference_iterator_dict_without_references():
    tester = {
        "foo": 42,
        "bar": "baz",
        "baz": {
            "quux": {
                "fnord": False,
            },
        },
    }
    frozen = tuple(iterators.reference_iterator(tester))
    assert len(frozen) == 0, "Found items when it should not have!"


def test_reference_iterator_dict_with_references():
    tester = {
        "foo": 42,
        "bar": "baz",
        "$ref": "root",
        "baz": {
            "$ref": "branch",
            "quux": {
                "fnord": False,
                "$ref": "leaf",
            },
        },
    }
    frozen = tuple(iterators.reference_iterator(tester))
    assert len(frozen) == 3, "Found a different item count than expected!"

    # We have three references with their paths here, but we don't know which
    # order the tuple has them in. Let's go through them all:
    expectations = {
        0: "root",
        1: "branch",
        2: "leaf",
    }
    for key, value, path in iterators.reference_iterator(tester):
        assert value == expectations[len(path)]
