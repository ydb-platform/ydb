from pyrsistent import pset, s
import pytest
import pickle


def test_literalish_works():
    assert s() is pset()
    assert s(1, 2) == pset([1, 2])


def test_supports_hash():
    assert hash(s(1, 2)) == hash(s(1, 2))


def test_empty_truthiness():
    assert s(1)
    assert not s()


def test_contains_elements_that_it_was_initialized_with():
    initial = [1, 2, 3]
    s = pset(initial)

    assert set(s) == set(initial)
    assert len(s) == len(set(initial))


def test_is_immutable():
    s1 = pset([1])
    s2 = s1.add(2)

    assert s1 == pset([1])
    assert s2 == pset([1, 2])

    s3 = s2.remove(1)
    assert s2 == pset([1, 2])
    assert s3 == pset([2])


def test_remove_when_not_present():
    s1 = s(1, 2, 3)
    with pytest.raises(KeyError):
        s1.remove(4)


def test_discard():
    s1 = s(1, 2, 3)
    assert s1.discard(3) == s(1, 2)
    assert s1.discard(4) is s1


def test_is_iterable():
    assert sum(pset([1, 2, 3])) == 6


def test_contains():
    s = pset([1, 2, 3])

    assert 2 in s
    assert 4 not in s


def test_supports_set_operations():
    s1 = pset([1, 2, 3])
    s2 = pset([3, 4, 5])

    assert s1 | s2 == s(1, 2, 3, 4, 5)
    assert s1.union(s2) == s1 | s2

    assert s1 & s2 == s(3)
    assert s1.intersection(s2) == s1 & s2

    assert s1 - s2 == s(1, 2)
    assert s1.difference(s2) == s1 - s2

    assert s1 ^ s2 == s(1, 2, 4, 5)
    assert s1.symmetric_difference(s2) == s1 ^ s2


def test_supports_set_comparisons():
    s1 = s(1, 2, 3)
    s3 = s(1, 2)
    s4 = s(1, 2, 3)

    assert s(1, 2, 3, 3, 5) == s(1, 2, 3, 5)
    assert s1 != s3

    assert s3 < s1
    assert s3 <= s1
    assert s3 <= s4

    assert s1 > s3
    assert s1 >= s3
    assert s4 >= s3


def test_str():
    rep = str(pset([1, 2, 3]))
    assert rep == "pset([1, 2, 3])"


def test_is_disjoint():
    s1 = pset([1, 2, 3])
    s2 = pset([3, 4, 5])
    s3 = pset([4, 5])

    assert not s1.isdisjoint(s2)
    assert s1.isdisjoint(s3)


def test_evolver_simple_add():
    x = s(1, 2, 3)
    e = x.evolver()
    assert not e.is_dirty()

    e.add(4)
    assert e.is_dirty()

    x2 = e.persistent()
    assert not e.is_dirty()
    assert x2 == s(1, 2, 3, 4)
    assert x == s(1, 2, 3)

def test_evolver_simple_remove():
    x = s(1, 2, 3)
    e = x.evolver()
    e.remove(2)

    x2 = e.persistent()
    assert x2 == s(1, 3)
    assert x == s(1, 2, 3)


def test_evolver_no_update_produces_same_pset():
    x = s(1, 2, 3)
    e = x.evolver()
    assert e.persistent() is x


def test_evolver_len():
    x = s(1, 2, 3)
    e = x.evolver()
    assert len(e) == 3


def test_copy_returns_reference_to_self():
    s1 = s(10)
    assert s1.copy() is s1


def test_pickling_empty_set():
    assert pickle.loads(pickle.dumps(s(), -1)) == s()


def test_pickling_non_empty_map():
    assert pickle.loads(pickle.dumps(s(1, 2), -1)) == s(1, 2)


def test_supports_weakref():
    import weakref
    weakref.ref(s(1))


def test_update():
    assert s(1, 2, 3).update([3, 4, 4, 5]) == s(1, 2, 3, 4, 5)


def test_update_no_elements():
    s1 = s(1, 2)
    assert s1.update([]) is s1


def test_iterable():
    """
    PSets can be created from iterables even though they can't be len() hinted.
    """

    assert pset(iter("a")) == pset(iter("a"))
