import numpy as np  # only run this test suite if numpy is installed
import pytest
from h3.api import (
    basic_int,
    numpy_int,
    memview_int,
)

# todo: check when a copy is made, and when it isn't


def test_set():
    ints = {
        619056821839331327,
        619056821839593471,
        619056821839855615,
        619056821840117759,
        619056821840379903,
        619056821840642047,
        619056821840904191,
    }

    h = 614553222213795839

    assert basic_int.compact(ints) == {h}

    with pytest.raises(TypeError):
        # numpy can't convert from a set
        numpy_int.compact(ints)

    with pytest.raises(TypeError):
        # set isn't a memoryview
        memview_int.compact(ints)


def test_list():
    ints = [
        619056821839331327,
        619056821839593471,
        619056821839855615,
        619056821840117759,
        619056821840379903,
        619056821840642047,
        619056821840904191,
    ]

    h = 614553222213795839

    assert basic_int.compact(ints) == {h}

    # numpy can convert from a list OK
    # (numpy knows to convert it to uint64)
    assert numpy_int.compact(ints) == np.array([h], dtype='uint64')

    # little weird that numpy comparisons don't consider dtype
    assert numpy_int.compact(ints) == np.array([h])
    assert not numpy_int.compact(ints).dtype == np.array([h]).dtype

    with pytest.raises(TypeError):
        # list isn't a memoryview
        memview_int.compact(ints)


def test_np_array():
    ints = np.array([
        619056821839331327,
        619056821839593471,
        619056821839855615,
        619056821840117759,
        619056821840379903,
        619056821840642047,
        619056821840904191,
    ], dtype='uint64')

    h = 614553222213795839

    assert basic_int.compact(ints) == {h}

    assert numpy_int.compact(ints) == np.array([h], dtype='uint64')
    assert numpy_int.compact(ints).dtype == np.dtype('uint64')

    out = memview_int.compact(ints)
    assert len(out) == 1
    assert out[0] == h


def test_list_to_array():
    ints0 = [
        619056821839331327,
        619056821839593471,
        619056821839855615,
        619056821840117759,
        619056821840379903,
        619056821840642047,
        619056821840904191,
    ]

    ints = np.array(ints0)

    h = 614553222213795839

    assert basic_int.compact(ints) == {h}
    assert numpy_int.compact(ints) == np.array([h], dtype='uint64')

    with pytest.raises(ValueError):
        # Without the explicit dtype given above, the array
        # assumes it has *signed* integers
        # The `memview_int` interface requires a dtype match
        # with uint64.
        memview_int.compact(ints)


def test_iterator():

    def foo():
        ints = iter([
            619056821839331327,
            619056821839593471,
            619056821839855615,
            619056821840117759,
            619056821840379903,
            619056821840642047,
            619056821840904191,
        ])

        return ints

    h = 614553222213795839

    ints = foo()
    assert basic_int.compact(ints) == {h}

    ints = foo()
    with pytest.raises(TypeError):
        # numpy can't create an array from an iterator
        numpy_int.compact(ints)

    ints = foo()
    with pytest.raises(TypeError):
        # requires a bytes-like input
        memview_int.compact(ints)
