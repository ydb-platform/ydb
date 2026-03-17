import numpy
import pytest
from hypothesis import given

from thinc.api import Padded, Ragged, get_width
from thinc.types import ArgsKwargs
from thinc.util import (
    convert_recursive,
    get_array_module,
    is_cupy_array,
    is_numpy_array,
    to_categorical,
)

from . import strategies

ALL_XP = [numpy]
try:
    import cupy

    ALL_XP.append(cupy)
except ImportError:
    pass


@pytest.mark.parametrize(
    "obj,width",
    [
        (numpy.zeros((1, 2, 3, 4)), 4),
        (numpy.array(1), 0),
        (numpy.array([1, 2]), 3),
        ([numpy.zeros((1, 2)), numpy.zeros((1))], 2),
        (Ragged(numpy.zeros((1, 2)), numpy.zeros(1)), 2),  # type:ignore
        (
            Padded(
                numpy.zeros((2, 1, 2)),  # type:ignore
                numpy.zeros(2),  # type:ignore
                numpy.array([1, 0]),  # type:ignore
                numpy.array([0, 1]),  # type:ignore
            ),
            2,
        ),
        ([], 0),
    ],
)
def test_get_width(obj, width):
    assert get_width(obj) == width


@pytest.mark.parametrize("obj", [1234, "foo", {"a": numpy.array(0)}])
def test_get_width_fail(obj):
    with pytest.raises(ValueError):
        get_width(obj)


@pytest.mark.parametrize("xp", ALL_XP)
def test_array_module_cpu_gpu_helpers(xp):
    error = (
        "Only numpy and cupy arrays are supported"
        ", but found <class 'int'> instead. If "
        "get_array_module module wasn't called "
        "directly, this might indicate a bug in Thinc."
    )
    with pytest.raises(ValueError, match=error):
        get_array_module(0)
    zeros = xp.zeros((1, 2))
    xp_ = get_array_module(zeros)
    assert xp_ == xp
    if xp == numpy:
        assert is_numpy_array(zeros)
        assert not is_numpy_array((1, 2))
    else:
        assert is_cupy_array(zeros)
        assert not is_cupy_array((1, 2))


@given(
    label_smoothing=strategies.floats(min_value=0.0, max_value=0.5, exclude_max=True)
)
def test_to_categorical(label_smoothing):
    # Test without n_classes
    one_hot = to_categorical(numpy.asarray([1, 2], dtype="i"))
    assert one_hot.shape == (2, 3)
    # From keras
    # https://github.com/keras-team/keras/blob/master/tests/keras/utils/np_utils_test.py
    nc = 5
    shapes = [(1,), (3,), (4, 3), (5, 4, 3), (3, 1), (3, 2, 1)]
    expected_shapes = [
        (1, nc),
        (3, nc),
        (4, 3, nc),
        (5, 4, 3, nc),
        (3, 1, nc),
        (3, 2, 1, nc),
    ]
    labels = [numpy.random.randint(0, nc, shape) for shape in shapes]
    one_hots = [to_categorical(label, nc) for label in labels]
    smooths = [
        to_categorical(label, nc, label_smoothing=label_smoothing) for label in labels
    ]
    for i in range(len(expected_shapes)):
        label = labels[i]
        one_hot = one_hots[i]
        expected_shape = expected_shapes[i]
        smooth = smooths[i]
        assert one_hot.shape == expected_shape
        assert smooth.shape == expected_shape
        assert numpy.array_equal(one_hot, one_hot.astype(bool))
        assert numpy.all(one_hot.sum(axis=-1) == 1)
        assert numpy.all(numpy.argmax(one_hot, -1).reshape(label.shape) == label)
        assert numpy.all(smooth.argmax(axis=-1) == one_hot.argmax(axis=-1))
        assert numpy.all(numpy.isclose(numpy.sum(smooth, axis=-1), 1.0))
        assert numpy.isclose(numpy.max(smooth), 1 - label_smoothing)
        assert numpy.isclose(
            numpy.min(smooth), label_smoothing / (smooth.shape[-1] - 1)
        )

    # At least one class is required without label smoothing.
    numpy.testing.assert_allclose(
        to_categorical(numpy.asarray([0, 0, 0]), 1), [[1.0], [1.0], [1.0]]
    )
    numpy.testing.assert_allclose(
        to_categorical(numpy.asarray([0, 0, 0])), [[1.0], [1.0], [1.0]]
    )
    with pytest.raises(ValueError, match=r"n_classes should be at least 1"):
        to_categorical(numpy.asarray([0, 0, 0]), 0)

    # At least two classes are required with label smoothing.
    numpy.testing.assert_allclose(
        to_categorical(numpy.asarray([0, 1, 0]), 2, label_smoothing=0.01),
        [[0.99, 0.01], [0.01, 0.99], [0.99, 0.01]],
    )
    numpy.testing.assert_allclose(
        to_categorical(numpy.asarray([0, 1, 0]), label_smoothing=0.01),
        [[0.99, 0.01], [0.01, 0.99], [0.99, 0.01]],
    )
    with pytest.raises(
        ValueError, match=r"n_classes should be greater than 1.*label smoothing.*but 1"
    ):
        to_categorical(numpy.asarray([0, 1, 0]), 1, label_smoothing=0.01),
    with pytest.raises(
        ValueError, match=r"n_classes should be greater than 1.*label smoothing.*but 1"
    ):
        to_categorical(numpy.asarray([0, 0, 0]), label_smoothing=0.01),

    with pytest.raises(ValueError, match=r"label_smoothing parameter"):
        to_categorical(numpy.asarray([0, 1, 2, 3, 4]), label_smoothing=0.8)

    with pytest.raises(ValueError, match=r"label_smoothing parameter"):
        to_categorical(numpy.asarray([0, 1, 2, 3, 4]), label_smoothing=0.88)


def test_convert_recursive():
    is_match = lambda obj: obj == "foo"
    convert_item = lambda obj: obj.upper()
    obj = {
        "a": {("b", "foo"): {"c": "foo", "d": ["foo", {"e": "foo", "f": (1, "foo")}]}}
    }
    result = convert_recursive(is_match, convert_item, obj)
    assert result["a"][("b", "FOO")]["c"] == "FOO"
    assert result["a"][("b", "FOO")]["d"] == ["FOO", {"e": "FOO", "f": (1, "FOO")}]
    obj = {"a": ArgsKwargs(("foo", [{"b": "foo"}]), {"a": ["x", "foo"]})}
    result = convert_recursive(is_match, convert_item, obj)
    assert result["a"].args == ("FOO", [{"b": "FOO"}])
    assert result["a"].kwargs == {"a": ["x", "FOO"]}
