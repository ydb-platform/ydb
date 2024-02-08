"""Tests for freeze and thaw."""
import collections
from pyrsistent import v, m, s, freeze, thaw, PRecord, field, mutant


## Freeze (standard)

def test_freeze_basic():
    assert freeze(1) == 1
    assert freeze('foo') == 'foo'

def test_freeze_list():
    assert freeze([1, 2]) == v(1, 2)

def test_freeze_dict():
    result = freeze({'a': 'b'})
    assert result == m(a='b')
    assert type(freeze({'a': 'b'})) is type(m())

def test_freeze_defaultdict():
    test_dict = collections.defaultdict(dict)
    test_dict['a'] = 'b'
    result = freeze(test_dict)
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

def test_freeze_recurse_in_defaultdict_values():
    test_dict = collections.defaultdict(dict)
    test_dict['a'] = [1]
    result = freeze(test_dict)
    assert result == m(a=v(1))
    assert type(result['a']) is type(v())

def test_freeze_recurse_in_pmap_values():
    input = {'a': m(b={'c': 1})}
    result = freeze(input)
    # PMap and PVector are == to their mutable equivalents
    assert result == input
    assert type(result) is type(m())
    assert type(result['a']['b']) is type(m())

def test_freeze_recurse_in_lists():
    result = freeze(['a', {'b': 3}])
    assert result == v('a', m(b=3))
    assert type(result[1]) is type(m())

def test_freeze_recurse_in_pvectors():
    input = [1, v(2, [3])]
    result = freeze(input)
    # PMap and PVector are == to their mutable equivalents
    assert result == input
    assert type(result) is type(v())
    assert type(result[1][1]) is type(v())

def test_freeze_recurse_in_tuples():
    """Values in tuples are recursively frozen."""
    result = freeze(('a', {}))
    assert result == ('a', m())
    assert type(result[1]) is type(m())


## Freeze (weak)

def test_freeze_nonstrict_no_recurse_in_pmap_values():
    input = {'a': m(b={'c': 1})}
    result = freeze(input, strict=False)
    # PMap and PVector are == to their mutable equivalents
    assert result == input
    assert type(result) is type(m())
    assert type(result['a']['b']) is dict

def test_freeze_nonstrict_no_recurse_in_pvectors():
    input = [1, v(2, [3])]
    result = freeze(input, strict=False)
    # PMap and PVector are == to their mutable equivalents
    assert result == input
    assert type(result) is type(v())
    assert type(result[1][1]) is list


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

def test_thaw_recurse_in_dict_values():
    result = thaw({'a': v(1, m(b=2))})
    assert result == {'a': [1, {'b': 2}]}
    assert type(result['a']) is list
    assert type(result['a'][1]) is dict

def test_thaw_recurse_in_vectors():
    result = thaw(v('a', m(b=3)))
    assert result == ['a', {'b': 3}]
    assert type(result[1]) is dict

def test_thaw_recurse_in_lists():
    result = thaw(v(['a', m(b=1), v(2)]))
    assert result == [['a', {'b': 1}, [2]]]
    assert type(result[0]) is list
    assert type(result[0][1]) is dict

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


## Thaw (weak)

def test_thaw_non_strict_no_recurse_in_dict_values():
    result = thaw({'a': v(1, m(b=2))}, strict=False)
    assert result == {'a': [1, {'b': 2}]}
    assert type(result['a']) is type(v())
    assert type(result['a'][1]) is type(m())

def test_thaw_non_strict_no_recurse_in_lists():
    result = thaw(v(['a', m(b=1), v(2)]), strict=False)
    assert result == [['a', {'b': 1}, [2]]]
    assert type(result[0][1]) is type(m())

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
