import contextlib
import shutil
import tempfile
from pathlib import Path

import numpy
import pytest

from thinc.api import ArgsKwargs, Linear, Padded, Ragged
from thinc.util import has_cupy, is_cupy_array, is_numpy_array


@contextlib.contextmanager
def make_tempdir():
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(str(d))


def get_model(W_b_input, cls=Linear):
    W, b, input_ = W_b_input
    nr_out, nr_in = W.shape
    model = cls(nr_out, nr_in)
    model.set_param("W", W)
    model.set_param("b", b)
    model.initialize()
    return model


def get_shape(W_b_input):
    W, b, input_ = W_b_input
    return input_.shape[0], W.shape[0], W.shape[1]


def get_data_checker(inputs):
    if isinstance(inputs, Ragged):
        return assert_raggeds_match
    elif isinstance(inputs, Padded):
        return assert_paddeds_match
    elif isinstance(inputs, list):
        return assert_lists_match
    elif isinstance(inputs, tuple) and len(inputs) == 4:
        return assert_padded_data_match
    elif isinstance(inputs, tuple) and len(inputs) == 2:
        return assert_ragged_data_match
    else:
        return assert_arrays_match


def assert_arrays_match(X, Y):
    assert X.dtype == Y.dtype
    # Transformations are allowed to change last dimension, but not batch size.
    assert X.shape[0] == Y.shape[0]
    return True


def assert_lists_match(X, Y):
    assert isinstance(X, list)
    assert isinstance(Y, list)
    assert len(X) == len(Y)
    for x, y in zip(X, Y):
        assert_arrays_match(x, y)
    return True


def assert_raggeds_match(X, Y):
    assert isinstance(X, Ragged)
    assert isinstance(Y, Ragged)
    assert_arrays_match(X.lengths, Y.lengths)
    assert_arrays_match(X.data, Y.data)
    return True


def assert_paddeds_match(X, Y):
    assert isinstance(X, Padded)
    assert isinstance(Y, Padded)
    assert_arrays_match(X.size_at_t, Y.size_at_t)
    assert assert_arrays_match(X.lengths, Y.lengths)
    assert assert_arrays_match(X.indices, Y.indices)
    assert X.data.dtype == Y.data.dtype
    assert X.data.shape[1] == Y.data.shape[1]
    assert X.data.shape[0] == Y.data.shape[0]
    return True


def assert_padded_data_match(X, Y):
    return assert_paddeds_match(Padded(*X), Padded(*Y))


def assert_ragged_data_match(X, Y):
    return assert_raggeds_match(Ragged(*X), Ragged(*Y))


def check_input_converters(Y, backprop, data, n_args, kwargs_keys, type_):
    assert isinstance(Y, ArgsKwargs)
    assert len(Y.args) == n_args
    assert list(Y.kwargs.keys()) == kwargs_keys
    assert all(isinstance(arg, type_) for arg in Y.args)
    assert all(isinstance(arg, type_) for arg in Y.kwargs.values())
    dX = backprop(Y)

    def is_supported_backend_array(arr):
        return is_cupy_array(arr) or is_numpy_array(arr)

    input_type = type(data) if not isinstance(data, list) else tuple
    assert isinstance(dX, input_type) or is_supported_backend_array(dX)

    if isinstance(data, dict):
        assert list(dX.keys()) == kwargs_keys
        assert all(is_supported_backend_array(arr) for arr in dX.values())
    elif isinstance(data, (list, tuple)):
        assert isinstance(dX, tuple)
        assert all(is_supported_backend_array(arr) for arr in dX)
    elif isinstance(data, ArgsKwargs):
        assert len(dX.args) == n_args
        assert list(dX.kwargs.keys()) == kwargs_keys

        assert all(is_supported_backend_array(arg) for arg in dX.args)
        assert all(is_supported_backend_array(arg) for arg in dX.kwargs.values())
    elif not isinstance(data, numpy.ndarray):
        pytest.fail(f"Bad data type: {dX}")
