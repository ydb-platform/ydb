from __future__ import annotations

import random

import hypothesis.strategies as st
import pytest
from hypothesis import given, settings

import rapidfuzz.distance._initialize_cpp as distance_cpp
import rapidfuzz.distance._initialize_py as distance_py
from rapidfuzz.distance import Editops, Levenshtein, Opcodes


def test_editops_comparison():
    """
    test comparison with Editops
    """
    ops = Levenshtein.editops("aaabaaa", "abbaaabba")
    assert ops == ops
    assert not (ops != ops)  # noqa: SIM202
    assert ops == ops.copy()
    assert not (ops != ops.copy())  # noqa: SIM202


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_editops_get_index(module):
    """
    test __getitem__ with index of Editops
    """
    ops = module.Editops(
        [
            ("delete", 1, 1),
            ("replace", 2, 1),
            ("insert", 6, 5),
            ("insert", 6, 6),
            ("insert", 6, 7),
        ],
        7,
        9,
    )

    ops_list = [
        ("delete", 1, 1),
        ("replace", 2, 1),
        ("insert", 6, 5),
        ("insert", 6, 6),
        ("insert", 6, 7),
    ]

    assert ops[0] == ops_list[0]
    assert ops[1] == ops_list[1]
    assert ops[2] == ops_list[2]
    assert ops[3] == ops_list[3]
    assert ops[4] == ops_list[4]

    assert ops[-1] == ops_list[-1]
    assert ops[-2] == ops_list[-2]
    assert ops[-3] == ops_list[-3]
    assert ops[-4] == ops_list[-4]
    assert ops[-5] == ops_list[-5]

    with pytest.raises(IndexError):
        ops[5]
    with pytest.raises(IndexError):
        ops[-6]


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_editops_get_slice(module):
    """
    test __getitem__ with slice of Editops
    """
    ops = module.Editops(
        [
            ("delete", 1, 1),
            ("replace", 2, 1),
            ("insert", 6, 5),
            ("insert", 6, 6),
            ("insert", 6, 7),
        ],
        7,
        9,
    )

    ops_list = [
        ("delete", 1, 1),
        ("replace", 2, 1),
        ("insert", 6, 5),
        ("insert", 6, 6),
        ("insert", 6, 7),
    ]

    assert ops[::].as_list() == ops_list[::]
    assert ops[::] is not ops
    assert ops[1:].as_list() == ops_list[1:]
    assert ops[:3].as_list() == ops_list[:3]
    assert ops[1:3].as_list() == ops_list[1:3]
    assert ops[:-1].as_list() == ops_list[:-1]
    assert ops[-3:].as_list() == ops_list[-3:]
    assert ops[-3:-1].as_list() == ops_list[-3:-1]

    with pytest.raises(ValueError, match="slice step cannot be zero"):
        ops[::0]

    with pytest.raises(ValueError, match="step sizes below 0 lead to an invalid order of editops"):
        ops[::-1]


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_editops_del_slice(module):
    """
    test __delitem__ with slice of Editops
    """
    ops = module.Editops(
        [
            ("delete", 1, 1),
            ("replace", 2, 1),
            ("insert", 6, 5),
            ("insert", 6, 6),
            ("insert", 6, 7),
        ],
        7,
        9,
    )

    ops_list = [
        ("delete", 1, 1),
        ("replace", 2, 1),
        ("insert", 6, 5),
        ("insert", 6, 6),
        ("insert", 6, 7),
    ]

    def del_test(key):
        _ops = ops[::]
        _ops_list = ops_list[::]
        del _ops[key]
        del _ops_list[key]
        assert _ops.as_list() == _ops_list

    del_test(slice(None, 4, None))
    del_test(slice(1, None, None))
    del_test(slice(1, 4, None))
    del_test(slice(None, 4, 2))
    del_test(slice(1, None, 2))
    del_test(slice(1, 4, 2))

    del_test(slice(None, -1, None))
    del_test(slice(-4, None, None))
    del_test(slice(-4, -1, None))
    del_test(slice(None, -1, 2))
    del_test(slice(-4, None, 2))
    del_test(slice(-4, -1, 2))


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_editops_inversion(module):
    """
    test correct inversion of Editops
    """
    ops = module.Editops(
        [
            ("delete", 1, 1),
            ("replace", 2, 1),
            ("insert", 6, 5),
            ("insert", 6, 6),
            ("insert", 6, 7),
        ],
        7,
        9,
    )

    assert ops.inverse().as_list() == [
        ("insert", 1, 1),
        ("replace", 1, 2),
        ("delete", 5, 6),
        ("delete", 6, 6),
        ("delete", 7, 6),
    ]


def test_opcodes_comparison():
    """
    test comparison with Opcodes
    """
    ops = Levenshtein.opcodes("aaabaaa", "abbaaabba")
    assert ops == ops
    assert not (ops != ops)  # noqa: SIM202
    assert ops == ops.copy()
    assert not (ops != ops.copy())  # noqa: SIM202


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_opcode_py_get_index(module):
    """
    test __getitem__ with index of Opcodes
    """
    ops = module.Opcodes(
        [
            ("equal", 0, 1, 0, 1),
            ("delete", 1, 2, 1, 1),
            ("replace", 2, 3, 1, 2),
            ("equal", 3, 6, 2, 5),
            ("insert", 6, 6, 5, 8),
            ("equal", 6, 7, 8, 9),
        ],
        7,
        9,
    )

    ops_list = [
        ("equal", 0, 1, 0, 1),
        ("delete", 1, 2, 1, 1),
        ("replace", 2, 3, 1, 2),
        ("equal", 3, 6, 2, 5),
        ("insert", 6, 6, 5, 8),
        ("equal", 6, 7, 8, 9),
    ]

    assert ops[0] == ops_list[0]
    assert ops[1] == ops_list[1]
    assert ops[2] == ops_list[2]
    assert ops[3] == ops_list[3]
    assert ops[4] == ops_list[4]
    assert ops[5] == ops_list[5]

    assert ops[-1] == ops_list[-1]
    assert ops[-2] == ops_list[-2]
    assert ops[-3] == ops_list[-3]
    assert ops[-4] == ops_list[-4]
    assert ops[-5] == ops_list[-5]
    assert ops[-6] == ops_list[-6]

    with pytest.raises(IndexError):
        ops[6]
    with pytest.raises(IndexError):
        ops[-7]


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_opcode_inversion(module):
    """
    test correct inversion of Opcodes
    """
    ops = module.Opcodes(
        [
            ("equal", 0, 1, 0, 1),
            ("delete", 1, 2, 1, 1),
            ("replace", 2, 3, 1, 2),
            ("equal", 3, 6, 2, 5),
            ("insert", 6, 6, 5, 8),
            ("equal", 6, 7, 8, 9),
        ],
        7,
        9,
    )

    assert ops.inverse().as_list() == [
        ("equal", 0, 1, 0, 1),
        ("insert", 1, 1, 1, 2),
        ("replace", 1, 2, 2, 3),
        ("equal", 2, 5, 3, 6),
        ("delete", 5, 8, 6, 6),
        ("equal", 8, 9, 6, 7),
    ]


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_opcodes_empty(module):
    """
    test behavior of conversion between empty list and Editops
    """
    ops = module.Opcodes([], 0, 0)
    assert ops.as_list() == []
    assert ops.src_len == 0
    assert ops.dest_len == 0

    ops = module.Opcodes([], 0, 3)
    assert ops.as_list() == [module.Opcode(tag="equal", src_start=0, src_end=0, dest_start=0, dest_end=3)]
    assert ops.src_len == 0
    assert ops.dest_len == 3


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_editops_empty(module):
    """
    test behavior of conversion between empty list and Opcodes
    """
    ops = module.Editops([], 0, 0)
    assert ops.as_list() == []
    assert ops.src_len == 0
    assert ops.dest_len == 0

    ops = module.Editops([], 0, 3)
    assert ops.as_list() == []
    assert ops.src_len == 0
    assert ops.dest_len == 3


def test_list_initialization():
    """
    test whether list initialization works correctly
    """
    ops = Levenshtein.opcodes("aaabaaa", "abbaaabba")
    ops2 = Opcodes(ops.as_list(), ops.src_len, ops.dest_len)
    assert ops == ops2

    ops = Levenshtein.editops("aaabaaa", "abbaaabba")
    ops2 = Editops(ops.as_list(), ops.src_len, ops.dest_len)
    assert ops == ops2

    ops = Levenshtein.opcodes("aaabaaa", "abbaaabba")
    ops2 = Editops(ops.as_list(), ops.src_len, ops.dest_len)
    assert ops.as_editops() == ops2

    ops = Levenshtein.editops("aaabaaa", "abbaaabba")
    ops2 = Opcodes(ops.as_list(), ops.src_len, ops.dest_len)
    assert ops.as_opcodes() == ops2

    ops = Levenshtein.editops("skdsakldsakdlasda", "djkajkdfkdgkhdfjrmecsidjf")
    ops2 = Opcodes(ops.as_list(), ops.src_len, ops.dest_len)
    assert ops.as_opcodes() == ops2


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_merge_adjacent_blocks(module):
    """
    test whether adjacent blocks are merged
    """
    ops1 = [module.Opcode(tag="equal", src_start=0, src_end=3, dest_start=0, dest_end=3)]
    ops2 = [
        module.Opcode(tag="equal", src_start=0, src_end=1, dest_start=0, dest_end=1),
        module.Opcode(tag="equal", src_start=1, src_end=3, dest_start=1, dest_end=3),
    ]
    assert module.Opcodes(ops1, 3, 3) == module.Opcodes(ops2, 3, 3)
    assert module.Opcodes(ops2, 3, 3) == module.Opcodes(ops2, 3, 3).as_editops().as_opcodes()


@pytest.mark.parametrize("module", [distance_py, distance_cpp])
def test_empty_matching_blocks(module):
    """
    test behavior for empty matching blocks
    """
    assert module.Editops([], 0, 0).as_matching_blocks() == [module.MatchingBlock(a=0, b=0, size=0)]
    assert module.Editops([], 0, 3).as_matching_blocks() == [module.MatchingBlock(a=0, b=3, size=0)]
    assert module.Editops([], 3, 0).as_matching_blocks() == [module.MatchingBlock(a=3, b=0, size=0)]

    assert module.Opcodes([], 0, 0).as_matching_blocks() == [module.MatchingBlock(a=0, b=0, size=0)]
    assert module.Opcodes([], 0, 3).as_matching_blocks() == [module.MatchingBlock(a=0, b=3, size=0)]
    assert module.Opcodes([], 3, 0).as_matching_blocks() == [module.MatchingBlock(a=3, b=0, size=0)]


@given(s1=st.text(), s2=st.text())
@settings(max_examples=100, deadline=None)
def test_editops_reversible(s1, s2):
    """
    test whether conversions between editops and opcodes are reversible
    """
    ops = Levenshtein.editops(s1, s2)
    assert ops == ops.as_opcodes().as_editops()
    while ops:
        del ops[random.randrange(len(ops))]
        assert Opcodes(ops.as_list(), ops.src_len, ops.dest_len) == ops.as_opcodes()
        assert ops == ops.as_opcodes().as_editops()
