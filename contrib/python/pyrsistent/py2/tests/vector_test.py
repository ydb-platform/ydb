import os
import pickle
import pytest

from pyrsistent._pvector import python_pvector


@pytest.fixture(scope='session', params=['pyrsistent._pvector', 'pvectorc'])
def pvector(request):
    if request.param == 'pvectorc' and os.environ.get('PYRSISTENT_NO_C_EXTENSION'):
        pytest.skip('Configured to not run tests for C extension')

    m = pytest.importorskip(request.param)
    if request.param == 'pyrsistent._pvector':
        return m.python_pvector
    return m.pvector


def test_literalish_works():
    from pyrsistent import pvector, v
    assert v() is pvector()
    assert v(1, 2) == pvector([1, 2])


def test_empty_initialization(pvector):
    seq = pvector()
    assert len(seq) == 0

    with pytest.raises(IndexError) as error:
        x = seq[0]
    assert str(error.value) == 'Index out of range: 0'


def test_initialization_with_one_element(pvector):
    seq = pvector([3])
    assert len(seq) == 1
    assert seq[0] == 3


def test_append_works_and_does_not_affect_original_within_tail(pvector):
    seq1 = pvector([3])
    seq2 = seq1.append(2)

    assert len(seq1) == 1
    assert seq1[0] == 3

    assert len(seq2) == 2
    assert seq2[0] == 3
    assert seq2[1] == 2


def test_append_works_and_does_not_affect_original_outside_tail(pvector):
    original = pvector([])
    seq = original

    for x in range(33):
        seq = seq.append(x)

    assert len(seq) == 33
    assert seq[0] == 0
    assert seq[31] == 31
    assert seq[32] == 32

    assert len(original) == 0


def test_append_when_root_overflows(pvector):
    seq = pvector([])

    for x in range(32 * 33):
        seq = seq.append(x)

    seq = seq.append(10001)

    for i in range(32 * 33):
        assert seq[i] == i

    assert seq[32 * 33] == 10001


def test_multi_level_sequence(pvector):
    seq = pvector(range(8000))
    seq2 = seq.append(11)

    assert seq[5] == 5
    assert seq2[7373] == 7373
    assert seq2[8000] == 11


def test_multi_level_sequence_from_iterator(pvector):
    seq = pvector(iter(range(8000)))
    seq2 = seq.append(11)

    assert seq[5] == 5
    assert seq2[7373] == 7373
    assert seq2[8000] == 11


def test_random_insert_within_tail(pvector):
    seq = pvector([1, 2, 3])

    seq2 = seq.set(1, 4)

    assert seq2[1] == 4
    assert seq[1] == 2


def test_random_insert_outside_tail(pvector):
    seq = pvector(range(20000))

    seq2 = seq.set(19000, 4)

    assert seq2[19000] == 4
    assert seq[19000] == 19000


def test_insert_beyond_end(pvector):
    seq = pvector(range(2))
    seq2 = seq.set(2, 50)
    assert seq2[2] == 50

    with pytest.raises(IndexError) as error:
        seq2.set(19, 4)

    assert str(error.value) == 'Index out of range: 19'


def test_insert_with_index_from_the_end(pvector):
    x = pvector([1, 2, 3, 4])

    assert x.set(-2, 5) == pvector([1, 2, 5, 4])


def test_insert_with_too_negative_index(pvector):
    x = pvector([1, 2, 3, 4])

    with pytest.raises(IndexError):
        x.set(-5, 17)


def test_iteration(pvector):
    y = 0
    seq = pvector(range(2000))
    for x in seq:
        assert x == y
        y += 1

    assert y == 2000


def test_zero_extend(pvector):
    the_list = []
    seq = pvector()
    seq2 = seq.extend(the_list)
    assert seq == seq2


def test_short_extend(pvector):
    # Extend within tail length
    the_list = [1, 2]
    seq = pvector()
    seq2 = seq.extend(the_list)

    assert len(seq2) == len(the_list)
    assert seq2[0] == the_list[0]
    assert seq2[1] == the_list[1]


def test_long_extend(pvector):
    # Multi level extend
    seq = pvector()
    length = 2137

    # Extend from scratch
    seq2 = seq.extend(range(length))
    assert len(seq2) == length
    for i in range(length):
        assert seq2[i] == i

    # Extend already filled vector
    seq3 = seq2.extend(range(length, length + 5))
    assert len(seq3) == length + 5
    for i in range(length + 5):
        assert seq3[i] == i

    # Check that the original vector is still intact
    assert len(seq2) == length
    for i in range(length):
        assert seq2[i] == i


def test_slicing_zero_length_range(pvector):
    seq = pvector(range(10))
    seq2 = seq[2:2]

    assert len(seq2) == 0


def test_slicing_range(pvector):
    seq = pvector(range(10))
    seq2 = seq[2:4]

    assert list(seq2) == [2, 3]


def test_slice_identity(pvector):
    # Pvector is immutable, no need to make a copy!
    seq = pvector(range(10))

    assert seq is seq[::]


def test_slicing_range_with_step(pvector):
    seq = pvector(range(100))
    seq2 = seq[2:12:3]

    assert list(seq2) == [2, 5, 8, 11]


def test_slicing_no_range_but_step(pvector):
    seq = pvector(range(10))
    seq2 = seq[::2]

    assert list(seq2) == [0, 2, 4, 6, 8]


def test_slicing_reverse(pvector):
    seq = pvector(range(10))
    seq2 = seq[::-1]

    assert seq2[0] == 9
    assert seq2[1] == 8
    assert len(seq2) == 10

    seq3 = seq[-3: -7: -1]
    assert seq3[0] == 7
    assert seq3[3] == 4
    assert len(seq3) == 4


def test_delete_index(pvector):
    seq = pvector([1, 2, 3])
    assert seq.delete(0) == pvector([2, 3])
    assert seq.delete(1) == pvector([1, 3])
    assert seq.delete(2) == pvector([1, 2])
    assert seq.delete(-1) == pvector([1, 2])
    assert seq.delete(-2) == pvector([1, 3])
    assert seq.delete(-3) == pvector([2, 3])


def test_delete_index_out_of_bounds(pvector):
    with pytest.raises(IndexError):
        pvector([]).delete(0)
    with pytest.raises(IndexError):
        pvector([]).delete(-1)


def test_delete_index_malformed(pvector):
    with pytest.raises(TypeError):
        pvector([]).delete('a')


def test_delete_slice(pvector):
    seq = pvector(range(5))
    assert seq.delete(1, 4) == pvector([0, 4])
    assert seq.delete(4, 1) == seq
    assert seq.delete(0, 1) == pvector([1, 2, 3, 4])
    assert seq.delete(6, 8) == seq
    assert seq.delete(-1, 1) == seq
    assert seq.delete(1, -1) == pvector([0, 4])


def test_remove(pvector):
    seq = pvector(range(5))
    assert seq.remove(3) == pvector([0, 1, 2, 4])


def test_remove_first_only(pvector):
    seq = pvector([1, 2, 3, 2, 1])
    assert seq.remove(2) == pvector([1, 3, 2, 1])


def test_remove_index_out_of_bounds(pvector):
    seq = pvector(range(5))
    with pytest.raises(ValueError) as err:
        seq.remove(5)
    assert 'not in' in str(err.value)


def test_addition(pvector):
    v = pvector([1, 2]) + pvector([3, 4])

    assert list(v) == [1, 2, 3, 4]


def test_sorted(pvector):
    seq = pvector([5, 2, 3, 1])
    assert [1, 2, 3, 5] == sorted(seq)


def test_boolean_conversion(pvector):
    assert not bool(pvector())
    assert bool(pvector([1]))


def test_access_with_negative_index(pvector):
    seq = pvector([1, 2, 3, 4])

    assert seq[-1] == 4
    assert seq[-4] == 1


def test_index_error_positive(pvector):
    with pytest.raises(IndexError):
        pvector([1, 2, 3])[3]


def test_index_error_negative(pvector):
    with pytest.raises(IndexError):
        pvector([1, 2, 3])[-4]


def test_is_sequence(pvector):
    from pyrsistent._compat import Sequence
    assert isinstance(pvector(), Sequence)


def test_empty_repr(pvector):
    assert str(pvector()) == "pvector([])"


def test_non_empty_repr(pvector):
    v = pvector([1, 2, 3])
    assert str(v) == "pvector([1, 2, 3])"

    # There's some state that needs to be reset between calls in the native version,
    # test that multiple invocations work.
    assert str(v) == "pvector([1, 2, 3])"


def test_repr_when_contained_object_contains_reference_to_self(pvector):
    x = [1, 2, 3]
    v = pvector([1, 2, x])
    x.append(v)
    assert str(v) == 'pvector([1, 2, [1, 2, 3, pvector([1, 2, [...]])]])'

    # Run a GC to provoke any potential misbehavior
    import gc
    gc.collect()


def test_is_hashable(pvector):
    from pyrsistent._compat import Hashable
    v = pvector([1, 2, 3])
    v2 = pvector([1, 2, 3])

    assert hash(v) == hash(v2)
    assert isinstance(pvector(), Hashable)


def test_refuses_to_hash_when_members_are_unhashable(pvector):
    v = pvector([1, 2, [1, 2]])

    with pytest.raises(TypeError):
        hash(v)


def test_compare_same_vectors(pvector):
    v = pvector([1, 2])
    assert v == v
    assert pvector() == pvector()


def test_compare_with_other_type_of_object(pvector):
    assert pvector([1, 2]) != 'foo'


def test_compare_equal_vectors(pvector):
    v1 = pvector([1, 2])
    v2 = pvector([1, 2])
    assert v1 == v2
    assert v1 >= v2
    assert v1 <= v2


def test_compare_different_vectors_same_size(pvector):
    v1 = pvector([1, 2])
    v2 = pvector([1, 3])
    assert v1 != v2


def test_compare_different_vectors_different_sizes(pvector):
    v1 = pvector([1, 2])
    v2 = pvector([1, 2, 3])
    assert v1 != v2


def test_compare_lt_gt(pvector):
    v1 = pvector([1, 2])
    v2 = pvector([1, 2, 3])
    assert v1 < v2
    assert v2 > v1


def test_repeat(pvector):
    v = pvector([1, 2])
    assert 5 * pvector() is pvector()
    assert v is 1 * v
    assert 0 * v is pvector()
    assert 2 * pvector([1, 2]) == pvector([1, 2, 1, 2])
    assert -3 * pvector([1, 2]) is pvector()


def test_transform_zero_key_length(pvector):
    x = pvector([1, 2])

    assert x.transform([], 3) == 3


def test_transform_base_case(pvector):
    x = pvector([1, 2])

    assert x.transform([1], 3) == pvector([1, 3])


def test_transform_nested_vectors(pvector):
    x = pvector([1, 2, pvector([3, 4]), 5])

    assert x.transform([2, 0], 999) == pvector([1, 2, pvector([999, 4]), 5])


def test_transform_when_appending(pvector):
    from pyrsistent import m
    x = pvector([1, 2])

    assert x.transform([2, 'd'], 999) == pvector([1, 2, m(d=999)])


def test_transform_index_error_out_range(pvector):
    x = pvector([1, 2, pvector([3, 4]), 5])

    with pytest.raises(IndexError):
        x.transform([2, 10], 999)


def test_transform_index_error_wrong_type(pvector):
    x = pvector([1, 2, pvector([3, 4]), 5])

    with pytest.raises(TypeError):
        x.transform([2, 'foo'], 999)


def test_transform_non_setable_type(pvector):
    x = pvector([1, 2, 5])

    with pytest.raises(TypeError):
        x.transform([2, 3], 999)


def test_reverse(pvector):
    x = pvector([1, 2, 5])

    assert list(reversed(x)) == [5, 2, 1]


def test_contains(pvector):
    x = pvector([1, 2, 5])

    assert 2 in x
    assert 3 not in x


def test_index(pvector):
    x = pvector([1, 2, 5])

    assert x.index(5) == 2


def test_index_not_found(pvector):
    x = pvector([1, 2, 5])

    with pytest.raises(ValueError):
        x.index(7)


def test_index_not_found_with_limits(pvector):
    x = pvector([1, 2, 5, 1])

    with pytest.raises(ValueError):
        x.index(1, 1, 3)


def test_count(pvector):
    x = pvector([1, 2, 5, 1])

    assert x.count(1) == 2
    assert x.count(4) == 0


def test_empty_truthiness(pvector):
    assert pvector([1])
    assert not pvector([])


def test_pickling_empty_vector(pvector):
    assert pickle.loads(pickle.dumps(pvector(), -1)) == pvector()


def test_pickling_non_empty_vector(pvector):
    assert pickle.loads(pickle.dumps(pvector([1, 'a']), -1)) == pvector([1, 'a'])


def test_mset_basic_assignments(pvector):
    v1 = pvector(range(2000))
    v2 = v1.mset(1, -1, 505, -505, 1998, -1998)

    # Original not changed
    assert v1[1] == 1
    assert v1[505] == 505
    assert v1[1998] == 1998

    # Other updated
    assert v2[1] == -1
    assert v2[505] == -505
    assert v2[1998] == -1998


def test_mset_odd_number_of_arguments(pvector):
    v = pvector([0, 1])

    with pytest.raises(TypeError):
        v.mset(0, 10, 1)


def test_mset_index_out_of_range(pvector):
    v = pvector([0, 1])

    with pytest.raises(IndexError):
        v.mset(3, 10)


def test_evolver_no_update(pvector):
    # This is mostly a test against memory leaks in the C implementation
    v = pvector(range(40))

    assert v.evolver().persistent() == v


def test_evolver_deallocate_dirty_evolver(pvector):
    # Ref count handling in native implementation
    v = pvector(range(3220))
    e = v.evolver()
    e[10] = -10
    e[3220] = -3220


def test_evolver_simple_update_in_tree(pvector):
    v = pvector(range(35))
    e = v.evolver()
    e[10] = -10

    assert e[10] == -10
    assert e.persistent()[10] == -10


def test_evolver_set_out_of_range(pvector):
    v = pvector([0])
    e = v.evolver()
    with pytest.raises(IndexError) as error:
        e[10] = 1
    assert str(error.value) == "Index out of range: 10"

def test_evolver_multi_level_multi_update_in_tree(pvector):
    # This test is mostly to detect memory/ref count issues in the native implementation
    v = pvector(range(3500))
    e = v.evolver()

    # Update differs between first and second time since the
    # corresponding node will be marked as dirty the first time only.
    e[10] = -10
    e[11] = -11
    e[10] = -1000

    # Update in neighbour node
    e[50] = -50
    e[50] = -5000

    # Update in node in other half of vector
    e[3000] = -3000
    e[3000] = -30000

    # Before freezing
    assert e[10] == -1000
    assert e[11] == -11
    assert e[50] == -5000
    assert e[3000] == -30000

    # Run a GC to provoke any potential misbehavior
    import gc
    gc.collect()

    v2 = e.persistent()
    assert v2[10] == -1000
    assert v2[50] == -5000
    assert v2[3000] == -30000

    # Run a GC to provoke any potential misbehavior
    gc.collect()

    # After freezing
    assert e[10] == -1000
    assert e[11] == -11
    assert e[50] == -5000
    assert e[3000] == -30000

    # Original stays the same
    assert v[10] == 10
    assert v[50] == 50
    assert v[3000] == 3000


def test_evolver_simple_update_in_tail(pvector):
    v = pvector(range(35))
    e = v.evolver()
    e[33] = -33

    assert e[33] == -33
    assert e.persistent()[33] == -33
    assert v[33] == 33


def test_evolver_simple_update_just_outside_vector(pvector):
    v = pvector()
    e = v.evolver()
    e[0] = 1

    assert e[0] == 1
    assert e.persistent()[0] == 1
    assert len(v) == 0


def test_evolver_append(pvector):
    v = pvector()
    e = v.evolver()
    e.append(1000)
    assert e[0] == 1000

    e[0] = 2000
    assert e[0] == 2000
    assert list(e.persistent()) == [2000]
    assert list(v) == []


def test_evolver_extend(pvector):
    v = pvector([1000])
    e = v.evolver()
    e.extend([2000, 3000])
    e[2] = 20000

    assert list(e.persistent()) == [1000, 2000, 20000]
    assert list(v) == [1000]


def test_evolver_assign_and_read_with_negative_indices(pvector):
    v = pvector([1, 2, 3])
    e = v.evolver()
    e[-1] = 4
    e.extend([11, 12, 13])
    e[-1] = 33

    assert e[-1] == 33
    assert list(e.persistent()) == [1, 2, 4, 11, 12, 33]


def test_evolver_non_integral_access(pvector):
    e = pvector([1]).evolver()

    with pytest.raises(TypeError):
        x = e['foo']


def test_evolver_non_integral_assignment(pvector):
    e = pvector([1]).evolver()

    with pytest.raises(TypeError):
        e['foo'] = 1


def test_evolver_out_of_bounds_access(pvector):
    e = pvector([1]).evolver()

    with pytest.raises(IndexError):
        x = e[1]


def test_evolver_out_of_bounds_assignment(pvector):
    e = pvector([1]).evolver()

    with pytest.raises(IndexError):
        e[2] = 1


def test_no_dependencies_between_evolvers_from_the_same_pvector(pvector):
    original_list = list(range(40))
    v = pvector(original_list)
    e1 = v.evolver()
    e2 = v.evolver()

    e1.extend([1, 2, 3])
    e1[2] = 20
    e1[35] = 350

    e2.extend([-1, -2, -3])
    e2[2] = -20
    e2[35] = -350

    e1_expected = original_list + [1, 2, 3]
    e1_expected[2] = 20
    e1_expected[35] = 350
    assert list(e1.persistent()) == e1_expected

    e2_expected = original_list + [-1, -2, -3]
    e2_expected[2] = -20
    e2_expected[35] = -350
    assert list(e2.persistent()) == e2_expected


def test_pvectors_produced_from_the_same_evolver_do_not_interfere(pvector):
    original_list = list(range(40))
    v = pvector(original_list)
    e = v.evolver()

    e.extend([1, 2, 3])
    e[2] = 20
    e[35] = 350

    v1 = e.persistent()
    v1_expected = original_list + [1, 2, 3]
    v1_expected[2] = 20
    v1_expected[35] = 350

    e.extend([-1, -2, -3])
    e[3] = -30
    e[36] = -360

    v2 = e.persistent()
    v2_expected = v1_expected + [-1, -2, -3]
    v2_expected[3] = -30
    v2_expected[36] = -360

    assert list(v1) == v1_expected
    assert list(v2) == v2_expected


def test_evolver_len(pvector):
    e = pvector([1, 2, 3]).evolver()
    e.extend([4, 5])

    assert len(e) == 5


def test_evolver_is_dirty(pvector):
    e = pvector([1, 2, 3]).evolver()
    assert not e.is_dirty()

    e.append(4)
    assert e.is_dirty

    e.persistent()
    assert not e.is_dirty()

    e[2] = 2000
    assert e.is_dirty

    e.persistent()
    assert not e.is_dirty()


def test_vector_insert_one_step_beyond_end(pvector):
    # This test exists to get the transform functionality under memory
    # leak supervision. Most of the transformation tests are in test_transform.py.
    v = pvector([1, 2])
    assert v.transform([2], 3) == pvector([1, 2, 3])


def test_evolver_with_no_updates_returns_same_pvector(pvector):
    v = pvector([1, 2])
    assert v.evolver().persistent() is v


def test_evolver_returns_itself_on_evolving_operations(pvector):
    # Does this to be able to chain operations
    v = pvector([1, 2])
    assert v.evolver().append(3).extend([4, 5]).set(1, 6).persistent() == pvector([1, 6, 3, 4, 5])


def test_evolver_delete_by_index(pvector):
    e = pvector([1, 2, 3]).evolver()

    del e[0]

    assert e.persistent() == python_pvector([2, 3])
    assert e.append(4).persistent() == python_pvector([2, 3, 4])


def test_evolver_delete_function_by_index(pvector):
    e = pvector([1, 2, 3]).evolver()

    assert e.delete(1).persistent() == python_pvector([1, 3])


def test_evolver_delete_function_by_index_multiple_times(pvector):
    SIZE = 40
    e = pvector(range(SIZE)).evolver()
    for i in range(SIZE):
        assert e[0] == i
        assert list(e.persistent()) == list(range(i, SIZE))
        del e[0]

    assert e.persistent() == list()


def test_evolver_delete_function_invalid_index(pvector):
    e = pvector([1, 2]).evolver()

    with pytest.raises(TypeError):
        del e["e"]


def test_delete_of_non_existing_element(pvector):
    e = pvector([1, 2]).evolver()

    with pytest.raises(IndexError):
        del e[2]

    del e[0]
    del e[0]

    with pytest.raises(IndexError):
        del e[0]

    assert e.persistent() == pvector()


def test_append_followed_by_delete(pvector):
    e = pvector([1, 2]).evolver()

    e.append(3)

    del e[2]


def test_evolver_set_followed_by_delete(pvector):
    evolver = pvector([1, 2]).evolver()
    evolver[1] = 3

    assert [evolver[i] for i in range(len(evolver))] == [1, 3]

    del evolver[0]

    assert evolver.persistent() == pvector([3])


def test_compare_with_list(pvector):
    v = pvector([1, 2, 3])

    assert v == [1, 2, 3]
    assert v != [1, 2]
    assert v > [1, 2]
    assert v < [2, 2]
    assert [1, 2] < v
    assert v <= [1, 2, 3]
    assert v <= [1, 2, 4]
    assert v >= [1, 2, 3]
    assert v >= [1, 2]


def test_compare_with_non_iterable(pvector):
    assert pvector([1, 2, 3]) != 5
    assert not (pvector([1, 2, 3]) == 5)


def test_python_no_c_extension_with_environment_variable():
    from six.moves import reload_module
    import pyrsistent._pvector
    import pyrsistent
    import os

    os.environ['PYRSISTENT_NO_C_EXTENSION'] = 'TRUE'

    reload_module(pyrsistent._pvector)
    reload_module(pyrsistent)

    assert type(pyrsistent.pvector()) is pyrsistent._pvector.PythonPVector

    del os.environ['PYRSISTENT_NO_C_EXTENSION']

    reload_module(pyrsistent._pvector)
    reload_module(pyrsistent)


def test_supports_weakref(pvector):
    import weakref
    weakref.ref(pvector())

def test_get_evolver_referents(pvector):
    """The C implementation of the evolver should expose the original PVector
    to the gc only once.
    """
    if pvector.__module__ == 'pyrsistent._pvector':
        pytest.skip("This test only applies to pvectorc")
    import gc
    v = pvector([1, 2, 3])
    e = v.evolver()
    assert len([x for x in gc.get_referents(e) if x is v]) == 1


def test_failing_repr(pvector):
    # See https://github.com/tobgu/pyrsistent/issues/84
    class A(object):
        def __repr__(self):
            raise ValueError('oh no!')

    with pytest.raises(ValueError):
        repr(pvector([A()]))


def test_iterable(pvector):
    """
    PVectors can be created from iterables even though they can't be len()
    hinted.
    """

    assert pvector(iter("a")) == pvector(iter("a"))
