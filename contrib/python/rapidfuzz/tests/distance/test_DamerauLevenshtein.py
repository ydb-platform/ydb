from __future__ import annotations

import pytest

from rapidfuzz import utils_cpp, utils_py
from tests.distance.common import DamerauLevenshtein


@pytest.mark.parametrize(
    ("left", "right", "distance"),
    [
        ("test", "text", 1),
        ("test", "tset", 1),
        ("test", "qwy", 4),
        ("test", "testit", 2),
        ("test", "tesst", 1),
        ("test", "tet", 1),
        ("cat", "hat", 1),
        ("Niall", "Neil", 3),
        ("aluminum", "Catalan", 7),
        ("ATCG", "TAGC", 2),
        ("ab", "ba", 1),
        ("ab", "cde", 3),
        ("ab", "ac", 1),
        ("ab", "bc", 2),
        ("ca", "abc", 2),
    ],
)
def test_distance(left, right, distance):
    assert DamerauLevenshtein.distance(left, right) == distance


def testCaseInsensitive():
    assert DamerauLevenshtein.distance("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 0
    assert DamerauLevenshtein.distance("new york mets", "new YORK mets", processor=utils_py.default_process) == 0
