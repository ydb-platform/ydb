from __future__ import annotations

from rapidfuzz import utils_cpp, utils_py
from rapidfuzz.distance import metrics_cpp
from tests.distance.common import OSA


def isclose(a, b, rel_tol=1e-09, abs_tol=0.0):
    return abs(a - b) <= max(rel_tol * max(abs(a), abs(b)), abs_tol)


class CustomHashable:
    def __init__(self, string):
        self._string = string

    def __eq__(self, other):
        raise NotImplementedError

    def __ne__(self, other):
        return not self.__eq__(other)

    def __hash__(self):
        return hash(self._string)


def test_empty_string():
    """
    when both strings are empty this is a perfect match
    """
    assert OSA.distance("", "") == 0


def test_cross_type_matching():
    """
    strings should always be interpreted in the same way
    """
    assert OSA.distance("aaaa", "aaaa") == 0
    assert OSA.distance("aaaa", ["a", "a", "a", "a"]) == 0
    # todo add support in pure python
    assert metrics_cpp.osa_distance("aaaa", [ord("a"), ord("a"), "a", "a"]) == 0
    assert metrics_cpp.osa_distance([0, -1], [0, -2]) == 1
    assert (
        metrics_cpp.osa_distance(
            [CustomHashable("aa"), CustomHashable("aa")],
            [CustomHashable("aa"), CustomHashable("bb")],
        )
        == 1
    )


def test_word_error_rate():
    """
    it should be possible to use levenshtein to implement a word error rate
    """
    assert OSA.distance(["aaaaa", "bbbb"], ["aaaaa", "bbbb"]) == 0
    assert OSA.distance(["aaaaa", "bbbb"], ["aaaaa", "cccc"]) == 1


def test_simple():
    """
    some simple OSA specific tests
    """
    assert OSA.distance("CA", "ABC") == 3
    assert OSA.distance("CA", "AC") == 1
    assert OSA.distance("a" * 65 + "CA" + "a" * 65, "b" + "a" * 64 + "AC" + "a" * 64 + "b") == 3


def test_simple_unicode_tests():
    """
    some very simple tests using unicode with scorers
    to catch relatively obvious implementation errors
    """
    s1 = "ÁÄ"
    s2 = "ABCD"
    assert OSA.distance(s1, s2) == 4
    assert OSA.distance(s1, s1) == 0


def testCaseInsensitive():
    assert OSA.distance("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 0
    assert OSA.distance("new york mets", "new YORK mets", processor=utils_py.default_process) == 0
