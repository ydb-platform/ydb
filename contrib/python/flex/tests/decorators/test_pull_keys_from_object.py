from flex.constants import (
    EMPTY,
)
from flex.decorators import pull_keys_from_obj


def test_pulling_existing_keys():
    data = {
        'a': 1,
        'b': 2,
        'c': 3,
    }

    @pull_keys_from_obj('a', 'c')
    def fn(a, c):
        return a, c

    a, c = fn(data)
    assert a == 1
    assert c == 3


def test_pulling_missing_key():
    data = {
        'a': 1,
        #'b': 2,
        'c': 3,
    }

    @pull_keys_from_obj('a', 'b', 'c')
    def fn(a, b, c):
        return a, b, c

    a, b, c = fn(data)
    assert a == 1
    assert b is EMPTY
    assert c == 3


def test_noop_if_all_values_are_empty():
    @pull_keys_from_obj('a', 'b', 'c')
    def fn(a, b, c):
        assert False, 'This should not happen'

    fn({})
