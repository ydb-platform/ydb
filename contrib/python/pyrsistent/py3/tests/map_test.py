from collections import namedtuple
from collections.abc import Mapping, Hashable
from operator import add
import pytest
from pyrsistent import pmap, m
import pickle


def test_instance_of_hashable():
    assert isinstance(m(), Hashable)


def test_instance_of_map():
    assert isinstance(m(), Mapping)


def test_literalish_works():
    assert m() is pmap()
    assert m(a=1, b=2) == pmap({'a': 1, 'b': 2})


def test_empty_initialization():
    a_map = pmap()
    assert len(a_map) == 0


def test_initialization_with_one_element():
    the_map = pmap({'a': 2})
    assert len(the_map) == 1
    assert the_map['a'] == 2
    assert the_map.a == 2
    assert 'a' in the_map

    assert the_map is the_map.discard('b')

    empty_map = the_map.remove('a')
    assert len(empty_map) == 0
    assert 'a' not in empty_map


def test_get_non_existing_raises_key_error():
    m1 = m()
    with pytest.raises(KeyError) as error:
        m1['foo']

    assert str(error.value) == "'foo'"


def test_remove_non_existing_element_raises_key_error():
    m1 = m(a=1)

    with pytest.raises(KeyError) as error:
        m1.remove('b')

    assert str(error.value) == "'b'"


def test_various_iterations():
    assert {'a', 'b'} == set(m(a=1, b=2))
    assert ['a', 'b'] == sorted(m(a=1, b=2).keys())

    assert {1, 2} == set(m(a=1, b=2).itervalues())
    assert [1, 2] == sorted(m(a=1, b=2).values())

    assert {('a', 1), ('b', 2)} == set(m(a=1, b=2).iteritems())
    assert {('a', 1), ('b', 2)} == set(m(a=1, b=2).items())

    pm = pmap({k: k for k in range(100)})
    assert len(pm) == len(pm.keys())
    assert len(pm) == len(pm.values())
    assert len(pm) == len(pm.items())
    ks = pm.keys()
    assert all(k in pm for k in ks)
    assert all(k in ks for k in ks)
    us = pm.items()
    assert all(pm[k] == v for (k, v) in us)
    vs = pm.values()
    assert all(v in vs for v in vs)


def test_initialization_with_two_elements():
    map1 = pmap({'a': 2, 'b': 3})
    assert len(map1) == 2
    assert map1['a'] == 2
    assert map1['b'] == 3

    map2 = map1.remove('a')
    assert 'a' not in map2
    assert map2['b'] == 3


def test_initialization_with_many_elements():
    init_dict = dict([(str(x), x) for x in range(1700)])
    the_map = pmap(init_dict)

    assert len(the_map) == 1700
    assert the_map['16'] == 16
    assert the_map['1699'] == 1699
    assert the_map.set('256', 256) is the_map

    new_map = the_map.remove('1600')
    assert len(new_map) == 1699
    assert '1600' not in new_map
    assert new_map['1601'] == 1601

    # Some NOP properties
    assert new_map.discard('18888') is new_map
    assert '19999' not in new_map
    assert new_map['1500'] == 1500
    assert new_map.set('1500', new_map['1500']) is new_map


def test_access_non_existing_element():
    map1 = pmap()
    assert len(map1) == 0

    map2 = map1.set('1', 1)
    assert '1' not in map1
    assert map2['1'] == 1
    assert '2' not in map2


def test_overwrite_existing_element():
    map1 = pmap({'a': 2})
    map2 = map1.set('a', 3)

    assert len(map2) == 1
    assert map2['a'] == 3


def test_hash():
    x = m(a=1, b=2, c=3)
    y = m(a=1, b=2, c=3)

    assert hash(x) == hash(y)


def test_same_hash_when_content_the_same_but_underlying_vector_size_differs():
    x = pmap(dict((x, x) for x in range(1000)))
    y = pmap({10: 10, 200: 200, 700: 700})

    for z in x:
        if z not in y:
            x = x.remove(z)

    assert x == y
    assert hash(x) == hash(y)


class HashabilityControlled(object):
    hashable = True

    def __hash__(self):
        if self.hashable:
            return 4  # Proven random
        raise ValueError("I am not currently hashable.")


def test_map_does_not_hash_values_on_second_hash_invocation():
    hashable = HashabilityControlled()
    x = pmap(dict(el=hashable))
    hash(x)
    hashable.hashable = False
    hash(x)


def test_equal():
    x = m(a=1, b=2, c=3)
    y = m(a=1, b=2, c=3)

    assert x == y
    assert not (x != y)

    assert y == x
    assert not (y != x)


def test_equal_to_dict():
    x = m(a=1, b=2, c=3)
    y = dict(a=1, b=2, c=3)

    assert x == y
    assert not (x != y)

    assert y == x
    assert not (y != x)


def test_equal_with_different_bucket_sizes():
    x = pmap({'a': 1, 'b': 2}, 50)
    y = pmap({'a': 1, 'b': 2}, 10)

    assert x == y
    assert not (x != y)

    assert y == x
    assert not (y != x)


def test_equal_with_different_insertion_order():
    x = pmap([(i, i) for i in range(50)], 10)
    y = pmap([(i, i) for i in range(49, -1, -1)], 10)

    assert x == y
    assert not (x != y)

    assert y == x
    assert not (y != x)


def test_not_equal():
    x = m(a=1, b=2, c=3)
    y = m(a=1, b=2)

    assert x != y
    assert not (x == y)

    assert y != x
    assert not (y == x)


def test_not_equal_to_dict():
    x = m(a=1, b=2, c=3)
    y = dict(a=1, b=2, d=4)

    assert x != y
    assert not (x == y)

    assert y != x
    assert not (y == x)


def test_update_with_multiple_arguments():
    # If same value is present in multiple sources, the rightmost is used.
    x = m(a=1, b=2, c=3)
    y = x.update(m(b=4, c=5), {'c': 6})

    assert y == m(a=1, b=4, c=6)


def test_update_one_argument():
    x = m(a=1)

    assert x.update(m(b=2)) == m(a=1, b=2)


def test_update_no_arguments():
    x = m(a=1)

    assert x.update() is x


def test_addition():
    assert m(x=1, y=2) + m(y=3, z=4) == m(x=1, y=3, z=4)


def test_union_operator():
    assert m(x=1, y=2) | m(y=3, z=4) == m(x=1, y=3, z=4)


def test_transform_base_case():
    # Works as set when called with only one key
    x = m(a=1, b=2)

    assert x.transform(['a'], 3) == m(a=3, b=2)


def test_transform_nested_maps():
    x = m(a=1, b=m(c=3, d=m(e=6, f=7)))

    assert x.transform(['b', 'd', 'e'], 999) == m(a=1, b=m(c=3, d=m(e=999, f=7)))


def test_transform_levels_missing():
    x = m(a=1, b=m(c=3))

    assert x.transform(['b', 'd', 'e'], 999) == m(a=1, b=m(c=3, d=m(e=999)))


class HashDummy(object):
    def __hash__(self):
        return 6528039219058920  # Hash of '33'

    def __eq__(self, other):
        return self is other


def test_hash_collision_is_correctly_resolved():
    dummy1 = HashDummy()
    dummy2 = HashDummy()
    dummy3 = HashDummy()
    dummy4 = HashDummy()

    map1 = pmap({dummy1: 1, dummy2: 2, dummy3: 3})
    assert map1[dummy1] == 1
    assert map1[dummy2] == 2
    assert map1[dummy3] == 3
    assert dummy4 not in map1

    keys = set()
    values = set()
    for k, v in map1.iteritems():
        keys.add(k)
        values.add(v)

    assert keys == {dummy1, dummy2, dummy3}
    assert values == {1, 2, 3}

    map2 = map1.set(dummy1, 11)
    assert map2[dummy1] == 11

    # Re-use existing structure when inserted element is the same
    assert map2.set(dummy1, 11) is map2

    map3 = map1.set('a', 22)
    assert map3['a'] == 22
    assert map3[dummy3] == 3

    # Remove elements
    map4 = map1.discard(dummy2)
    assert len(map4) == 2
    assert map4[dummy1] == 1
    assert dummy2 not in map4
    assert map4[dummy3] == 3

    assert map1.discard(dummy4) is map1

    # Empty map handling
    empty_map = map4.remove(dummy1).remove(dummy3)
    assert len(empty_map) == 0
    assert empty_map.discard(dummy1) is empty_map


def test_bitmap_indexed_iteration():
    a_map = pmap({'a': 2, 'b': 1})
    keys = set()
    values = set()

    count = 0
    for k, v in a_map.iteritems():
        count += 1
        keys.add(k)
        values.add(v)

    assert count == 2
    assert keys == {'a', 'b'}
    assert values == {2, 1}


def test_iteration_with_many_elements():
    values = list(range(0, 2000))
    keys = [str(x) for x in values]
    init_dict = dict(zip(keys, values))

    hash_dummy1 = HashDummy()
    hash_dummy2 = HashDummy()

    # Throw in a couple of hash collision nodes to tests
    # those properly as well
    init_dict[hash_dummy1] = 12345
    init_dict[hash_dummy2] = 54321
    a_map = pmap(init_dict)

    actual_values = set()
    actual_keys = set()

    for k, v in a_map.iteritems():
        actual_values.add(v)
        actual_keys.add(k)

    assert actual_keys == set(keys + [hash_dummy1, hash_dummy2])
    assert actual_values == set(values + [12345, 54321])


def test_str():
    assert str(pmap({1: 2, 3: 4})) == "pmap({1: 2, 3: 4})"


def test_empty_truthiness():
    assert m(a=1)
    assert not m()


def test_update_with():
    assert m(a=1).update_with(add, m(a=2, b=4)) == m(a=3, b=4)
    assert m(a=1).update_with(lambda l, r: l, m(a=2, b=4)) == m(a=1, b=4)

    def map_add(l, r):
        return dict(list(l.items()) + list(r.items()))

    assert m(a={'c': 3}).update_with(map_add, m(a={'d': 4})) == m(a={'c': 3, 'd': 4})


def test_pickling_empty_map():
    assert pickle.loads(pickle.dumps(m(), -1)) == m()


def test_pickling_non_empty_map():
    assert pickle.loads(pickle.dumps(m(a=1, b=2), -1)) == m(a=1, b=2)


def test_set_with_relocation():
    x = pmap({'a': 1000}, pre_size=1)
    x = x.set('b', 3000)
    x = x.set('c', 4000)
    x = x.set('d', 5000)
    x = x.set('d', 6000)

    assert len(x) == 4
    assert x == pmap({'a': 1000, 'b': 3000, 'c': 4000, 'd': 6000})


def test_evolver_simple_update():
    x = m(a=1000, b=2000)
    e = x.evolver()
    e['b'] = 3000

    assert e['b'] == 3000
    assert e.persistent()['b'] == 3000
    assert x['b'] == 2000


def test_evolver_update_with_relocation():
    x = pmap({'a': 1000}, pre_size=1)
    e = x.evolver()
    e['b'] = 3000
    e['c'] = 4000
    e['d'] = 5000
    e['d'] = 6000

    assert len(e) == 4
    assert e.persistent() == pmap({'a': 1000, 'b': 3000, 'c': 4000, 'd': 6000})


def test_evolver_set_with_reallocation_edge_case():
    # Demonstrates a bug in evolver that also affects updates. Under certain
    # circumstances, the result of `x.update(y)` will **not** have all the
    # keys from `y`.
    foo = object()
    x = pmap({'a': foo}, pre_size=1)
    e = x.evolver()
    e['b'] = 3000
    # Bug is triggered when we do a reallocation and the new value is
    # identical to the old one.
    e['a'] = foo

    y = e.persistent()
    assert 'b' in y
    assert y is e.persistent()


def test_evolver_remove_element():
    e = m(a=1000, b=2000).evolver()
    assert 'a' in e

    del e['a']
    assert 'a' not in e


def test_evolver_remove_element_not_present():
    e = m(a=1000, b=2000).evolver()

    with pytest.raises(KeyError) as error:
        del e['c']

    assert str(error.value) == "'c'"


def test_copy_returns_reference_to_self():
    m1 = m(a=10)
    assert m1.copy() is m1


def test_dot_access_of_non_existing_element_raises_attribute_error():
    m1 = m(a=10)

    with pytest.raises(AttributeError) as error:
        m1.b

    error_message = str(error.value)

    assert "'b'" in error_message
    assert type(m1).__name__ in error_message


def test_pmap_unorderable():
    with pytest.raises(TypeError):
        _ = m(a=1) < m(b=2)

    with pytest.raises(TypeError):
        _ = m(a=1) <= m(b=2)

    with pytest.raises(TypeError):
        _ = m(a=1) > m(b=2)

    with pytest.raises(TypeError):
        _ = m(a=1) >= m(b=2)


def test_supports_weakref():
    import weakref
    weakref.ref(m(a=1))


def test_insert_and_get_many_elements():
    # This test case triggers reallocation of the underlying bucket structure.
    a_map = m()
    for x in range(1000):
        a_map = a_map.set(str(x), x)

    assert len(a_map) == 1000
    for x in range(1000):
        assert a_map[str(x)] == x, x


def test_iterable():
    """
    PMaps can be created from iterables even though they can't be len() hinted.
    """

    assert pmap(iter([("a", "b")])) == pmap([("a", "b")])


class BrokenPerson(namedtuple('Person', 'name')):
    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)


class BrokenItem(namedtuple('Item', 'name')):
    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)


def test_pmap_removal_with_broken_classes_deriving_from_namedtuple():
    """
    The two classes above implement __eq__ but also would need to implement __ne__ to compare
    consistently. See issue https://github.com/tobgu/pyrsistent/issues/268 for details.
    """
    s = pmap({BrokenPerson('X'): 2, BrokenItem('X'): 3})
    s = s.remove(BrokenPerson('X'))

    # Both items are removed due to how they are compared for inequality
    assert BrokenPerson('X') not in s
    assert BrokenItem('X') in s
    assert len(s) == 1
