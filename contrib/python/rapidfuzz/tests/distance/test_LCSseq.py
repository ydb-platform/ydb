from __future__ import annotations

from rapidfuzz import utils_cpp, utils_py
from tests.distance.common import LCSseq


def test_basic():
    assert LCSseq.distance("", "") == 0
    assert LCSseq.distance("test", "test") == 0
    assert LCSseq.distance("aaaa", "bbbb") == 4


def test_Editops():
    """
    basic test for LCSseq.editops
    """
    assert LCSseq.editops("0", "").as_list() == [("delete", 0, 0)]
    assert LCSseq.editops("", "0").as_list() == [("insert", 0, 0)]

    assert LCSseq.editops("00", "0").as_list() == [("delete", 1, 1)]
    assert LCSseq.editops("0", "00").as_list() == [("insert", 1, 1)]

    assert LCSseq.editops("qabxcd", "abycdf").as_list() == [
        ("delete", 0, 0),
        ("insert", 3, 2),
        ("delete", 3, 3),
        ("insert", 6, 5),
    ]
    assert LCSseq.editops("Lorem ipsum.", "XYZLorem ABC iPsum").as_list() == [
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

    ops = LCSseq.editops("aaabaaa", "abbaaabba")
    assert ops.src_len == 7
    assert ops.dest_len == 9


def testCaseInsensitive():
    assert LCSseq.distance("new york mets", "new YORK mets", processor=utils_cpp.default_process) == 0
    assert LCSseq.distance("new york mets", "new YORK mets", processor=utils_py.default_process) == 0
