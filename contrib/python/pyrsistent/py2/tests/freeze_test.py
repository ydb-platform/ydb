"""Tests for freeze and thaw."""

from pyrsistent import v, m, s, freeze, thaw, PRecord, field, mutant


## Freeze

def test_freeze_basic():
    assert freeze(1) == 1
    assert freeze('foo') == 'foo'

def test_freeze_list():
    assert freeze([1, 2]) == v(1, 2)

def test_freeze_dict():
    result = freeze({'a': 'b'})
    assert result == m(a='b')
    assert type(freeze({'a': 'b'})) is type(m())

def test_freeze_set():
    result = freeze(set([1, 2, 3]))
    assert result == s(1, 2, 3)
    assert type(result) is type(s())

def test_freeze_recurse_in_dictionary_values():
    result = freeze({'a': [1]})
    assert result == m(a=v(1))
    assert type(result['a']) is type(v())

def test_freeze_recurse_in_lists():
    result = freeze(['a', {'b': 3}])
    assert result == v('a', m(b=3))
    assert type(result[1]) is type(m())

def test_freeze_recurse_in_tuples():
    """Values in tuples are recursively frozen."""
    result = freeze(('a', {}))
    assert result == ('a', m())
    assert type(result[1]) is type(m())



## Thaw

def test_thaw_basic():
    assert thaw(1) == 1
    assert thaw('foo') == 'foo'

def test_thaw_list():
    result = thaw(v(1, 2))
    assert result == [1, 2]
    assert type(result) is list

def test_thaw_dict():
    result = thaw(m(a='b'))
    assert result == {'a': 'b'}
    assert type(result) is dict

def test_thaw_set():
    result = thaw(s(1, 2))
    assert result == set([1, 2])
    assert type(result) is set

def test_thaw_recurse_in_mapping_values():
    result = thaw(m(a=v(1)))
    assert result == {'a': [1]}
    assert type(result['a']) is list

def test_thaw_recurse_in_vectors():
    result = thaw(v('a', m(b=3)))
    assert result == ['a', {'b': 3}]
    assert type(result[1]) is dict

def test_thaw_recurse_in_tuples():
    result = thaw(('a', m()))
    assert result == ('a', {})
    assert type(result[1]) is dict

def test_thaw_can_handle_subclasses_of_persistent_base_types():
    class R(PRecord):
        x = field()

    result = thaw(R(x=1))
    assert result == {'x': 1}
    assert type(result) is dict


def test_mutant_decorator():
    @mutant
    def fn(a_list, a_dict):
        assert a_list == v(1, 2, 3)
        assert isinstance(a_dict, type(m()))
        assert a_dict == {'a': 5}

        return [1, 2, 3], {'a': 3}

    pv, pm = fn([1, 2, 3], a_dict={'a': 5})

    assert pv == v(1, 2, 3)
    assert pm == m(a=3)
    assert isinstance(pm, type(m()))
