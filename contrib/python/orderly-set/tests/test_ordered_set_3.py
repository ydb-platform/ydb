# The following tests are based on
# https://github.com/bustawin/ordered-set-37/blob/master/test/test_testing.py
# Thus shall be under the license:
# https://github.com/bustawin/ordered-set-37/blob/master/LICENSE.md

from pathlib import Path

import pytest

from . import all_sets, stable_and_orderly_sets, SortedSet, stable_and_orderly_sets_except_sorted_set

TESTS = Path(__file__).parent


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_add(set_t):
    x = set_t([1, 2, -1, "bar"])
    x.add(0)
    if set_t is SortedSet:
        assert list(x) == [-1, 0, 1, 2, 'bar']
    else:
        assert list(x) == [1, 2, -1, "bar", 0]


@pytest.mark.parametrize("set_t", all_sets)
def test_discard(set_t):
    x = set_t([1, 2, -1])
    x.discard(2)
    if set_t is SortedSet:
        assert list(x) == [-1, 1]
    else:
        assert list(x) == [1, -1]


@pytest.mark.parametrize("set_t", all_sets)
def test_discard_ignores_missing_element(set_t):
    x = set_t()
    x.discard(1)  # This does not raise


@pytest.mark.parametrize("set_t", all_sets)
def test_remove(set_t):
    x = set_t([1])
    x.remove(1)
    assert not x


@pytest.mark.parametrize("set_t", all_sets)
def test_remove_raises_missing_element(set_t):
    x = set_t()
    with pytest.raises(KeyError):
        x.remove(1)


@pytest.mark.parametrize("set_t", stable_and_orderly_sets_except_sorted_set)
def test_getitem(set_t):
    x = set_t([1, 2, -1])
    assert x[0] == 1
    assert x[1] == 2
    assert x[2] == -1
    with pytest.raises(IndexError):
        x[3]


@pytest.mark.parametrize("set_t", all_sets)
def test_len(set_t):
    x = set_t([1])
    assert len(x) == 1


@pytest.mark.parametrize("set_t", all_sets)
def test_iter(set_t):
    for x in set_t([1]):
        assert x == 1


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_str(set_t):
    x = set_t([1, 2, 3])
    assert str(x) == f"{set_t.__name__}([1, 2, 3])"


@pytest.mark.parametrize("set_t", stable_and_orderly_sets)
def test_repr(set_t):
    x = set_t([1, 2, 3])
    assert str(x) == f"{set_t.__name__}([1, 2, 3])"


@pytest.mark.parametrize("set_t", all_sets)
def test_eq(set_t):
    x = set_t([1, 2, 3])
    y = set_t([1, 2, 3])
    assert x == y
    assert x is not y


@pytest.mark.parametrize("set_t", all_sets)
def test_init_empty(set_t):
    x = set_t()
    assert len(x) == 0
    x.add(2)
    assert len(x) == 1


@pytest.mark.skip("Not ready yet")
@pytest.mark.parametrize("set_t", all_sets)
def test_typing_mypy(set_t, do_assert: bool = True):
    """Checks the typing values with mypy."""
    import mypy.api

    fixture = TESTS / "_test_mypy.py"
    module = TESTS.parent / "orderly_sets"
    *_, error = mypy.api.run([str(module), str(fixture)])
    if do_assert:
        assert not error
    else:
        print(*_)


if __name__ == "__main__":
    test_typing_mypy(False)
