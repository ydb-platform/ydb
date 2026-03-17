import h3.api.numpy_int as h3
import numpy as np  # only run this test suite if numpy is installed
import pytest

with pytest.warns(
    UserWarning,
    match = 'Modules under `h3.unstable` are experimental',
):
    import h3.unstable.vect as h3_vect


def test_h3_to_parent():
    # At res 9
    h = np.array([617700169958555647, 617700169958555647], np.uint64)

    # Default to res - 1
    arr1 = h3_vect.h3_to_parent(h)
    arr2 = h3_vect.h3_to_parent(h, 8)
    assert np.array_equal(arr1, arr2)

    # Same as other h3 bindings
    arr1 = h3_vect.h3_to_parent(h)
    arr2 = np.array(list(map(h3.h3_to_parent, h)), dtype=np.uint64)
    assert np.array_equal(arr1, arr2)

    # Test with a number passed to res
    arr1 = h3_vect.h3_to_parent(h, 7)
    arr2 = np.array([h3.h3_to_parent(c, 7) for c in h], dtype=np.uint64)
    assert np.array_equal(arr1, arr2)

    # Test with an array passed to res
    arr1 = h3_vect.h3_to_parent(h, np.array([7, 5], np.intc))
    arr2 = h3_vect.h3_to_parent(h, [7, 5])

    arr3 = []
    for c, res in zip(h, [7, 5]):
        arr3.append(h3.h3_to_parent(c, res))

    assert np.array_equal(arr1, arr2)
    assert np.array_equal(arr1, np.array(arr3, dtype=np.uint64))

    # Test with array-like (but not np.array) cell input
    arr1 = h3_vect.h3_to_parent(list(h))
    arr2 = np.array(list(map(h3.h3_to_parent, h)), dtype=np.uint64)
    assert np.array_equal(arr1, arr2)


def test_h3_to_parent_multiple_res():
    h = np.array([617700169958555647, 613196570331971583], np.uint64)

    # Cells at res 9, 8
    assert list(h3_vect.h3_get_resolution(h)) == [9, 8]

    # Same as other h3 bindings
    arr1 = h3_vect.h3_to_parent(h)
    arr2 = np.array(list(map(h3.h3_to_parent, h)), dtype=np.uint64)
    assert np.array_equal(arr1, arr2)

    # Parent cells are 8, 7
    parents = h3_vect.h3_to_parent(h)
    assert list(h3_vect.h3_get_resolution(parents)) == [8, 7]


def test_h3_get_resolution():
    h = np.array([617700169958555647], np.uint64)

    arr1 = h3_vect.h3_get_resolution(h)
    arr2 = np.array(list(map(h3.h3_get_resolution, h)), dtype=np.intc)
    assert np.array_equal(arr1, arr2)
