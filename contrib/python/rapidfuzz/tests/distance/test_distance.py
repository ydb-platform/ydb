from __future__ import annotations

import sys
from array import array

import pytest

from tests.distance.common import all_scorer_modules


class CustomHashable:
    def __init__(self, string):
        self._string = string

    def __eq__(self, other):
        raise NotImplementedError

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._string)


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_none(scorer):
    """
    All normalized scorers should be able to handle None values
    """
    assert scorer.normalized_distance(None, "test") == 1.0
    assert scorer.normalized_similarity(None, "test") == 0.0
    assert scorer.normalized_distance("test", None) == 1.0
    assert scorer.normalized_similarity("test", None) == 0.0


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_nan(scorer):
    """
    All normalized scorers should be able to handle float("nan")
    """
    assert scorer.normalized_distance(float("nan"), "test") == 1.0
    assert scorer.normalized_similarity(float("nan"), "test") == 0.0
    assert scorer.normalized_distance("test", float("nan")) == 1.0
    assert scorer.normalized_similarity("test", float("nan")) == 0.0


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_empty_strings(scorer):
    """
    Test behavior when comparing two empty strings
    """
    assert scorer.normalized_distance("", "") == 0.0
    assert scorer.normalized_similarity("", "") == 1.0


def unicodeArray(text):
    if sys.version_info >= (3, 13, 0):
        return array("w", list(text))
    return array("u", text)


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_similar_array(scorer):
    """
    arrays should be supported and treated in a compatible way to strings
    """
    assert (
        scorer.normalized_similarity(
            unicodeArray("the wonderful new york mets"),
            unicodeArray("the wonderful new york mets"),
        )
        == 1.0
    )
    assert (
        scorer.normalized_similarity("the wonderful new york mets", unicodeArray("the wonderful new york mets")) == 1.0
    )
    assert (
        scorer.normalized_similarity(unicodeArray("the wonderful new york mets"), "the wonderful new york mets") == 1.0
    )


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_similar_bytes(scorer):
    """
    bytes should be supported and treated in a compatible way to strings
    """
    assert scorer.normalized_similarity(b"the wonderful new york mets", b"the wonderful new york mets") == 1.0
    assert scorer.normalized_similarity("the wonderful new york mets", b"the wonderful new york mets") == 1.0
    assert scorer.normalized_similarity(b"the wonderful new york mets", "the wonderful new york mets") == 1.0


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_similar_ord_array(scorer):
    """
    elements in string should behave similar to their underlying unicode number
    """
    assert (
        scorer.normalized_similarity(
            [ord("a"), ord("a"), "a", "a"],
            [ord("a"), ord("a"), "a", "a"],
        )
        == 1.0
    )
    assert scorer.normalized_similarity("aaaa", [ord("a"), ord("a"), "a", "a"]) == 1.0
    assert scorer.normalized_similarity([ord("a"), ord("a"), "a", "a"], "aaaa") == 1.0


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_integer_array(scorer):
    """
    the numbers -1 and -2 should be treated differently (these are hash collisions in the standard has)
    """
    assert scorer.normalized_similarity([0, -1], [0, -2]) != 1.0


@pytest.mark.parametrize("scorer", all_scorer_modules)
def test_custom_hashable(scorer):
    """
    custom types should be compared using their hash
    """
    assert (
        scorer.normalized_similarity(
            [CustomHashable("aa"), CustomHashable("aa")],
            [CustomHashable("aa"), CustomHashable("aa")],
        )
        == 1.0
    )

    assert (
        scorer.normalized_similarity(
            [CustomHashable("aa"), CustomHashable("aa")],
            [CustomHashable("aa"), CustomHashable("bb")],
        )
        != 1.0
    )
