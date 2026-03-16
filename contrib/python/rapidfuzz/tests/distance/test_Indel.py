from __future__ import annotations

from rapidfuzz import utils_cpp, utils_py
from tests.distance.common import Indel


def test_basic():
    assert Indel.distance("", "") == 0
    assert Indel.distance("test", "test") == 0
    assert Indel.distance("aaaa", "bbbb") == 8


def test_issue_196():
    """
    Indel distance did not work correctly for score_cutoff=1
    """
    assert Indel.distance("South Korea", "North Korea") == 4
    assert Indel.distance("South Korea", "North Korea", score_cutoff=4) == 4
    assert Indel.distance("South Korea", "North Korea", score_cutoff=3) == 4
    assert Indel.distance("South Korea", "North Korea", score_cutoff=2) == 3
    assert Indel.distance("South Korea", "North Korea", score_cutoff=1) == 2
    assert Indel.distance("South Korea", "North Korea", score_cutoff=0) == 1


def test_Editops():
    """
    basic test for Indel.editops
    """
    assert Indel.editops("0", "").as_list() == [("delete", 0, 0)]
    assert Indel.editops("", "0").as_list() == [("insert", 0, 0)]

    assert Indel.editops("00", "0").as_list() == [("delete", 1, 1)]
    assert Indel.editops("0", "00").as_list() == [("insert", 1, 1)]

    assert Indel.editops("qabxcd", "abycdf").as_list() == [
        ("delete", 0, 0),
        ("insert", 3, 2),
        ("delete", 3, 3),
        ("insert", 6, 5),
    ]
    assert Indel.editops("Lorem ipsum.", "XYZLorem ABC iPsum").as_list() == [
        ("insert", 0, 0),
        ("insert", 0, 1),
        ("insert", 0, 2),
        ("insert", 6, 9),
        ("insert", 6, 10),
        ("insert", 6, 11),
        ("insert", 6, 12),
        ("insert", 7, 14),
        ("delete", 7, 15),
        ("delete", 11, 18),
    ]

    ops = Indel.editops("aaabaaa", "abbaaabba")
    assert ops.src_len == 7
    assert ops.dest_len == 9


def testCaseInsensitive():
    assert Indel.distance("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 0
    assert Indel.distance("new york mets", "new YORK mets", processor=utils_py.default_process) == 0
