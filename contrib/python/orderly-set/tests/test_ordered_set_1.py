import itertools as it
import json
import operator
import pickle
import random
import tempfile
from collections import OrderedDict, deque
from pathlib import Path

import pytest

from . import (
    StableSet,
    all_sets,
    all_sets_except_orderly_set,
    SortedSet,
    sets_and_stable_sets,
    stable_and_orderly_sets,
    stableeq_and_orderly_sets,
    OrderedSet,
    stable_and_orderly_sets_except_sorted_set,
)


@pytest.mark.parametrize("set_t", all_sets)
def test_pickle(set_t):
    set1 = set_t("abracadabra")
    roundtrip = pickle.loads(pickle.dumps(set1))
    assert roundtrip == set1


@pytest.mark.parametrize("set_t", all_sets)
def test_empty_pickle(set_t):
    empty_oset = set_t()
    empty_roundtrip = pickle.loads(pickle.dumps(empty_oset))
    assert empty_roundtrip == empty_oset


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_order(set_t):
    set1 = set_t("abracadabra")
    assert len(set1) == 5
    set2 = set_t(["a", "b", "r", "c", "d"])
    assert set1 == set2
    if set_t is SortedSet:
        assert list(reversed(set1)) == ['r', 'd', 'c', 'b', 'a']
    else:
        assert list(reversed(set1)) == ["d", "c", "r", "b", "a"]


@pytest.mark.parametrize("set_t", all_sets)
def test_binary_operations(set_t):
    set1 = set_t("abracadabra")
    set2 = set_t("simsalabim")
    assert set1 != set2

    assert set1 & set2 == set_t(["a", "b"])
    assert set1 | set2 == set_t(["a", "b", "r", "c", "d", "s", "i", "m", "l"])
    assert set1 - set2 == set_t(["r", "c", "d"])


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_indexing(set_t):
    set1 = set_t("abracadabra")
    assert set1[0] == "a"
    if set_t is SortedSet:
        assert set1[3] == "d"
    else:
        assert set1[3] == "c"
    assert set1[:] == set1
    assert set1.copy() == set1
    assert set1 is set1
    assert set1[:] is not set1
    assert set1.copy() is not set1

    assert set1.index("b") == 1
    if set_t is SortedSet:
        assert set1[[1, 2]] == ["b", "c"]
        assert set1[1:3] == set_t(["b", "c"])
        assert set1.indexes(["b", "r"]) == [1, 4]
    else:
        assert set1[[1, 2]] == ["b", "r"]
        assert set1[1:3] == set_t(["b", "r"])
        assert set1.indexes(["b", "r"]) == [1, 2]
    with pytest.raises(KeyError):
        set1.index("br")

    with pytest.raises(IndexError):
        set1[100]
        set1[[100, 0]]
        set1.index("br")


class FancyIndexTester:
    """
    Make sure we can index by a NumPy ndarray, without having to import
    NumPy.
    """

    def __init__(self, indices):
        self.indices = indices

    def __iter__(self):
        return iter(self.indices)

    def __index__(self):
        raise TypeError("NumPy arrays have weird __index__ methods")

    def __eq__(self, other):
        # Emulate NumPy being fussy about the == operator
        raise TypeError


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_fancy_index_class(set_t):
    set1 = set_t("abracadabra")
    indexer = FancyIndexTester([1, 0, 4, 3, 0, 2])
    if set_t is SortedSet:
        assert "".join(set1[indexer]) == "bardac"
    else:
        assert "".join(set1[indexer]) == "badcar"


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_indexes(set_t):
    set1 = set_t("abracadabra")
    assert set1.get_loc("b") == 1

    if set_t is SortedSet:
        assert set1.indexes(["b", "r"]) == [1, 4]
    else:
        assert set1.indexes(["b", "r"]) == [1, 2]


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_tuples(set_t):
    set1 = set_t()
    tup = ("tuple", 1)
    set1.add(tup)
    assert set1.index(tup) == 0
    assert set1[0] == tup


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_remove(set_t):
    set1 = set_t("abracadabra")

    set1.remove("a")
    set1.remove("b")

    assert set1 == set_t("rcd")
    if set_t is not SortedSet:
        assert set1[0] == "r"
        assert set1[1] == "c"
        assert set1[2] == "d"

        assert set1.index("r") == 0
        assert set1.index("c") == 1
        assert set1.index("d") == 2

    assert "a" not in set1
    assert "b" not in set1
    assert "r" in set1

    # Make sure we can .discard() something that's already gone, plus
    # something that was never there
    set1.discard("a")
    set1.discard("a")


@pytest.mark.parametrize("set_t", all_sets)
def test_remove_error(set_t):
    # If we .remove() an element that's not there, we get a KeyError
    set1 = set_t("abracadabra")
    with pytest.raises(KeyError):
        set1.remove("z")


@pytest.mark.parametrize("set_t", all_sets)
def test_clear(set_t):
    set1 = set_t("abracadabra")
    set1.clear()

    assert len(set1) == 0
    assert set1 == set_t()


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_update(set_t):
    set1 = set_t("abcd")
    result = set1.update("efgh")
    if set_t is not SortedSet:
        assert result == 7
    assert len(set1) == 8
    assert "".join(set1) == "abcdefgh"

    set2 = set_t("abcd")
    result = set2.update("cdef")
    if set_t is not SortedSet:
        assert result == 5
    assert len(set2) == 6
    assert "".join(set2) == "abcdef"


@pytest.mark.parametrize("set_t", stable_and_orderly_sets_except_sorted_set)
def test_pop(set_t):
    set1 = set_t("ab")
    elem = set1.pop()
    assert elem == "b"

    elem = set1.pop()
    assert elem == "a"

    pytest.raises(KeyError, set1.pop)


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_pop_by_index(set_t):
    set1 = set_t("abcde")
    elem = set1.pop(1)
    assert elem == "b"

    elem = set1.pop(1)
    assert elem == "c"

    elem = set1.pop(0)
    assert elem == "a"

    elem = set1.pop(-1)
    assert elem == "e"

    elem = set1.pop(-1)
    assert elem == "d"

    pytest.raises(KeyError, set1.pop)


@pytest.mark.parametrize("set_t", stable_and_orderly_sets_except_sorted_set)
def test_popitem(set_t):
    set1 = set_t("abcd")
    elem = set1.popitem()
    assert elem == "d"

    elem = set1.popitem(last=True)
    assert elem == "c"

    elem = set1.popitem(last=False)
    assert elem == "a"

    elem = set1.popitem()
    assert elem == "b"

    pytest.raises(KeyError, set1.popitem)


@pytest.mark.parametrize("set_t", stable_and_orderly_sets_except_sorted_set)
def test_move_to_end(set_t):
    set1 = set_t("abcd")
    set1.move_to_end("a")
    assert list(set1) == ["b", "c", "d", "a"]

    with pytest.raises(KeyError):
        set1.move_to_end("z")


@pytest.mark.parametrize("set_t", all_sets)
def test_getitem_type_error(set_t):
    set1 = set_t("ab")
    with pytest.raises(TypeError):
        set1["a"]


@pytest.mark.parametrize("set_t", all_sets)
def test_update_type_error(set_t):
    set1 = set_t("ab")
    with pytest.raises(TypeError):  # not ValueError
        # noinspection PyTypeChecker
        set1.update(3)


@pytest.mark.parametrize("set_t", all_sets)
def test_empty_repr(set_t):
    set1 = set_t()
    set_class_name = set_t.__name__
    assert repr(set1) == f"{set_class_name}()"


@pytest.mark.parametrize("set_t", all_sets)
def test_eq_wrong_type(set_t):
    set1 = set_t()
    assert set1 != 2


@pytest.mark.parametrize("set_t", all_sets)
def test_ordered_equality(set_t):
    # Ordered set checks order against sequences.
    set1 = set_t([1, 2])
    assert set1 == set_t([1, 2])
    if set_t is not StableSet:
        assert set1 == {1, 2}
        assert set1 == {2, 1}


@pytest.mark.parametrize("set_t", stableeq_and_orderly_sets)
def test_ordered_equality_non_set(set_t):
    set1 = set_t([1, 2])
    assert set1 == [1, 2]
    assert set1 == (1, 2)
    assert set1 == deque([1, 2])


@pytest.mark.parametrize("set_t", sets_and_stable_sets)
def test_ordered_equality_unorderly_sets(set_t):
    set1 = set_t([1, 2])
    assert set1 == set_t([2, 1])
    if set_t is not StableSet:
        assert set1 == {2, 1}


@pytest.mark.parametrize("set_t", [OrderedSet])
def test_ordered_inequality(set_t):
    # Equal Ordered set checks order against sequences.
    set1 = set_t([1, 2])
    assert set1 != set_t([2, 1])

    assert set1 != [2, 1]
    assert set1 != [2, 1, 1]

    assert set1 != (2, 1)
    assert set1 != (2, 1, 1)

    assert set1 != deque([2, 1])
    assert set1 != deque([2, 2, 1])


@pytest.mark.parametrize("set_t", all_sets)
def test_comparisons(set_t):
    # Comparison operators on sets actually test for subset and superset.
    assert set_t([1, 2]) < set_t([1, 2, 3])
    assert set_t([1, 2]) > set_t([1])

    assert set_t([1, 2]) > {1}


@pytest.mark.parametrize("set_t", all_sets)
def test_unordered_equality(set_t):
    # Unordered set checks order against non-sequences.
    if set_t is not StableSet:
        assert set_t([1, 2]) == {1, 2}
        assert set_t([1, 2]) == frozenset([2, 1])
        assert set_t([1, 2]) == {2: 2, 1: 1}.keys()
        assert set_t([1, 2]) == OrderedDict([(2, 2), (1, 1)]).keys()

    assert set_t([1, 2]) == {1: 1, 2: 2}.keys()



@pytest.mark.parametrize("set_t", [OrderedSet])
def test_unordered_equality_orderly_set(set_t):
    assert set_t([1, 2]) == {1: 1, 2: 2}.values()
    assert set_t([1, 2]) == {1: "a", 2: "b"}

    # Corner case: OrderedDict is not a Sequence, so we don't check for order,
    # even though it does have the concept of order.
    assert set_t([1, 2]) == OrderedDict([(2, 2), (1, 1)])

    # Corner case: We have to treat iterators as unordered because there
    # is nothing to distinguish an ordered and unordered iterator
    assert set_t([1, 2]) == iter([1, 2])
    assert set_t([1, 2]) == iter([2, 1])
    assert set_t([1, 2]) == iter([2, 1, 1])


@pytest.mark.parametrize("set_t", all_sets)
def test_unordered_inequality(set_t):
    assert set_t([1, 2]) != set([])
    assert set_t([1, 2]) != frozenset([2, 1, 3])

    assert set_t([1, 2]) != {2: "b"}
    assert set_t([1, 2]) != {1: 1, 4: 2}.keys()
    assert set_t([1, 2]) != {1: 1, 2: 3}.values()

    # Corner case: OrderedDict is not a Sequence, so we don't check for order,
    # even though it does have the concept of order.
    assert set_t([1, 2]) != OrderedDict([(2, 2), (3, 1)])


def allsame_(iterable, eq=operator.eq):
    """returns True of all items in iterable equal each other"""
    iter_ = iter(iterable)
    try:
        first = next(iter_)
    except StopIteration:
        return True
    return all(eq(first, item) for item in iter_)


def check_results_(results, datas, name):
    """
    helper for binary operator tests.

    check that all results have the same value, but are different items.
    data and name are used to indicate what sort of tests is run.
    """
    if not allsame_(results):
        raise AssertionError(
            "Not all same {} for {} with datas={}".format(results, name, datas)
        )
    for a, b in it.combinations(results, 2):
        if not isinstance(a, (bool, int)):
            assert a is not b, name + " should all be different items"


def _operator_consistency_testdata(set_t):
    """
    Predefined and random data used to test operator consistency.
    """
    # test case 1
    a = set_t([5, 3, 1, 4])
    b = set_t([1, 4])
    yield a, b

    # first set is empty
    a = set_t([])
    b = set_t([3, 1, 2])
    yield a, b

    # second set is empty
    a = set_t([3, 1, 2])
    b = set_t([])
    yield a, b

    # both sets are empty
    a = set_t([])
    b = set_t([])
    yield a, b

    # random test cases
    rng = random.Random(0)
    x, y = 20, 20
    for _ in range(10):
        a = set_t(rng.randint(0, x) for _ in range(y))
        b = set_t(rng.randint(0, x) for _ in range(y))
        yield a, b
        yield b, a


datasets_t = list(_operator_consistency_testdata(set_t) for set_t in all_sets)
datasets_t_except_orderly_set = list(_operator_consistency_testdata(set_t) for set_t in all_sets_except_orderly_set)
datasets = [item for sublist in datasets_t for item in sublist]
datasets_except_orderly_set = [item for sublist in datasets_t_except_orderly_set for item in sublist]




@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_isect(a, b):
    result1 = a.copy()
    result1.intersection_update(b)
    result2 = a & b
    result3 = a.intersection(b)
    check_results_([result1, result2, result3], datas=(a, b), name="isect")


@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_difference(a, b):
    result1 = a.copy()
    result1.difference_update(b)
    result2 = a - b
    result3 = a.difference(b)
    check_results_([result1, result2, result3], datas=(a, b), name="difference")


@pytest.mark.parametrize("a, b", datasets_except_orderly_set)
def test_operator_consistency_xor(a, b):
    result1 = a.copy()
    result1.symmetric_difference_update(b)
    result2 = a ^ b
    result3 = a.symmetric_difference(b)
    check_results_([result1, result2, result3], datas=(a, b), name="xor")


@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_union(a, b):
    result1 = a.copy()
    result1.update(b)
    result2 = a | b
    result3 = a.union(b)
    check_results_([result1, result2, result3], datas=(a, b), name="union")


@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_subset(a, b):
    result1 = a <= b
    result2 = a.issubset(b)
    result3 = set(a).issubset(set(b))
    check_results_([result1, result2, result3], datas=(a, b), name="subset")


@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_superset(a, b):
    result1 = a >= b
    result2 = a.issuperset(b)
    result3 = set(a).issuperset(set(b))
    check_results_([result1, result2, result3], datas=(a, b), name="superset")


@pytest.mark.parametrize("a, b", datasets)
def test_operator_consistency_disjoint(a, b):
    result1 = a.isdisjoint(b)
    result2 = len(a.intersection(b)) == 0
    check_results_([result1, result2], datas=(a, b), name="disjoint")


@pytest.mark.parametrize("set_t", all_sets)
def test_bitwise_and_consistency(set_t):
    # Specific case that was failing without explicit __and__ definition
    a = set_t([12, 13, 1, 8, 16, 15, 9, 11, 18, 6, 4, 3, 19, 17])
    b = set_t([19, 4, 9, 3, 2, 10, 15, 17, 11, 13, 20, 6, 14, 16, 8])
    result1 = a.copy()
    result1.intersection_update(b)
    # This requires a custom & operation apparently
    result2 = a & b
    result3 = a.intersection(b)
    check_results_([result1, result2, result3], datas=(a, b), name="isect")


@pytest.mark.skip("Not implemented yet")
@pytest.mark.parametrize("set_t", all_sets)
def test_json(set_t):
    a = set_t([1, 2, 3])
    json_data = json.dumps(a)
    b = json.loads(json_data)
    assert a == b


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_stability(set_t, override=False):
    """if you run this test twice it should fail with `set` and succeed with `StableSet`"""
    items = "abcdabcd"

    filename = Path(tempfile.gettempdir()) / f"data_{set_t.__name__}.pickle"
    if override or not filename.is_file():
        sitems0 = list(set_t(items))
        print(f"saving {filename}")
        print(sitems0)
        with open(filename, "wb") as f:
            pickle.dump(sitems0, f, pickle.HIGHEST_PROTOCOL)

    print(f"loading {filename}")
    with open(filename, "rb") as f:
        sitems = pickle.load(f)
        print(sitems)

    for i in range(100):
        new_sitems = list(set_t(items))
        print(new_sitems)
        assert sitems == new_sitems, f"{sitems} != {new_sitems}"


def test_equality_in_stable_set():
    oset = StableSet([1, 3, 2])
    assert oset == [1, 3, 2]
