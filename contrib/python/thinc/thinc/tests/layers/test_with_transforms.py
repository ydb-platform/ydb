import numpy
import numpy.testing
import pytest

from thinc.api import (
    Linear,
    Model,
    NumpyOps,
    noop,
    with_array,
    with_array2d,
    with_getitem,
    with_list,
    with_padded,
    with_ragged,
)
from thinc.types import Padded, Ragged

from ..util import get_data_checker


@pytest.fixture(params=[[], [(10, 2)], [(5, 3), (1, 3)], [(2, 3), (0, 3), (1, 3)]])
def shapes(request):
    return request.param


@pytest.fixture
def ops():
    return NumpyOps()


@pytest.fixture
def list_input(shapes):
    data = [numpy.zeros(shape, dtype="f") for shape in shapes]
    for i, x in enumerate(data):
        # Give values that make it easy to see where rows or columns mismatch.
        x += i * 100
        x += numpy.arange(x.shape[0]).reshape((-1, 1)) * 10
        x += numpy.arange(x.shape[1]).reshape((1, -1))
    return data


@pytest.fixture
def ragged_input(ops, list_input):
    lengths = numpy.array([len(x) for x in list_input], dtype="i")
    if not list_input:
        return Ragged(ops.alloc2f(0, 0), lengths)
    else:
        return Ragged(ops.flatten(list_input), lengths)


@pytest.fixture
def padded_input(ops, list_input):
    return ops.list2padded(list_input)


@pytest.fixture
def array_input(ragged_input):
    return ragged_input.data


@pytest.fixture
def padded_data_input(padded_input):
    x = padded_input
    return (x.data, x.size_at_t, x.lengths, x.indices)


@pytest.fixture
def ragged_data_input(ragged_input):
    return (ragged_input.data, ragged_input.lengths)


@pytest.fixture
def noop_models():
    return [
        with_padded(noop()),
        with_array(noop()),
        with_array2d(noop()),
        with_list(noop()),
        with_ragged(noop()),
    ]


# As an example operation, lets just trim the last dimension. That
# should catch stuff that confuses the input and output.


def get_array_model():
    def _trim_array_forward(model, X, is_train):
        def backprop(dY):
            return model.ops.alloc2f(dY.shape[0], dY.shape[1] + 1)

        return X[:, :-1], backprop

    return with_array2d(Model("trimarray", _trim_array_forward))


def get_list_model():
    def _trim_list_forward(model, Xs, is_train):
        def backprop(dYs):
            dXs = []
            for dY in dYs:
                dXs.append(model.ops.alloc2f(dY.shape[0], dY.shape[1] + 1))
            return dXs

        Ys = [X[:, :-1] for X in Xs]
        return Ys, backprop

    return with_list(Model("trimlist", _trim_list_forward))


def get_padded_model():
    def _trim_padded_forward(model, Xp, is_train):
        def backprop(dYp):
            dY = dYp.data
            dX = model.ops.alloc3f(dY.shape[0], dY.shape[1], dY.shape[2] + 1)
            return Padded(dX, dYp.size_at_t, dYp.lengths, dYp.indices)

        assert isinstance(Xp, Padded)
        X = Xp.data
        X = X.reshape((X.shape[0] * X.shape[1], X.shape[2]))
        X = X[:, :-1]
        X = X.reshape((Xp.data.shape[0], Xp.data.shape[1], X.shape[1]))
        return Padded(X, Xp.size_at_t, Xp.lengths, Xp.indices), backprop

    return with_padded(Model("trimpadded", _trim_padded_forward))


def get_ragged_model():
    def _trim_ragged_forward(model, Xr, is_train):
        def backprop(dYr):
            dY = dYr.data
            dX = model.ops.alloc2f(dY.shape[0], dY.shape[1] + 1)
            return Ragged(dX, dYr.lengths)

        return Ragged(Xr.data[:, :-1], Xr.lengths), backprop

    return with_ragged(Model("trimragged", _trim_ragged_forward))


def check_initialize(model, inputs):
    # Just check that these run and don't hit errors. I guess we should add a
    # spy and check that model.layers[0].initialize gets called, but shrug?
    model.initialize()
    model.initialize(X=inputs)
    model.initialize(X=inputs, Y=model.predict(inputs))


def check_transform_produces_correct_output_type_forward(model, inputs, checker):
    # It's pretty redundant to check these three assertions, so if the tests
    # get slow this could be removed. I think it should be fine though?
    outputs = model.predict(inputs)
    assert checker(inputs, outputs)
    outputs, _ = model(inputs, is_train=True)
    assert checker(inputs, outputs)
    outputs, _ = model(inputs, is_train=False)
    assert checker(inputs, outputs)


def check_transform_produces_correct_output_type_backward(model, inputs, checker):
    # It's pretty redundant to check these three assertions, so if the tests
    # get slow this could be removed. I think it should be fine though?
    outputs, backprop = model.begin_update(inputs)
    d_inputs = backprop(outputs)
    assert checker(inputs, d_inputs)


def check_transform_doesnt_change_noop_values(model, inputs, d_outputs):
    # Check that if we're wrapping a noop() layer in the transform, we don't
    # change the output values.
    outputs, backprop = model.begin_update(inputs)
    d_inputs = backprop(d_outputs)
    if isinstance(outputs, list):
        for i in range(len(outputs)):
            numpy.testing.assert_equal(inputs[i], outputs[i])
            numpy.testing.assert_equal(d_outputs[i], d_inputs[i])
    elif isinstance(outputs, numpy.ndarray):
        numpy.testing.assert_equal(inputs, outputs)
        numpy.testing.assert_equal(d_outputs, d_inputs)
    elif isinstance(outputs, Ragged):
        numpy.testing.assert_equal(inputs.data, outputs.data)
        numpy.testing.assert_equal(d_outputs.data, d_inputs.data)
    elif isinstance(outputs, Padded):
        numpy.testing.assert_equal(inputs.data, outputs.data)
        numpy.testing.assert_equal(d_inputs.data, d_inputs.data)


def test_noop_transforms(noop_models, ragged_input, padded_input, list_input):
    # Make distinct backprop values,
    # to check that the gradients get passed correctly
    d_ragged = Ragged(ragged_input.data + 1, ragged_input.lengths)
    d_padded = padded_input.copy()
    d_padded.data += 1
    d_list = [dx + 1 for dx in list_input]
    for model in noop_models:
        print(model.name)
        check_transform_doesnt_change_noop_values(model, padded_input, d_padded)
        check_transform_doesnt_change_noop_values(model, list_input, d_list)
        check_transform_doesnt_change_noop_values(model, ragged_input, d_ragged)


def test_with_array_initialize(ragged_input, padded_input, list_input, array_input):
    for inputs in (ragged_input, padded_input, list_input, array_input):
        check_initialize(get_array_model(), inputs)


def test_with_padded_initialize(
    ragged_input, padded_input, list_input, padded_data_input
):
    for inputs in (ragged_input, padded_input, list_input, padded_data_input):
        check_initialize(get_padded_model(), inputs)


def test_with_list_initialize(ragged_input, padded_input, list_input):
    for inputs in (ragged_input, padded_input, list_input):
        check_initialize(get_list_model(), inputs)


def test_with_ragged_initialize(
    ragged_input, padded_input, list_input, ragged_data_input
):
    for inputs in (ragged_input, padded_input, list_input, ragged_data_input):
        check_initialize(get_ragged_model(), inputs)


def test_with_array_forward(ragged_input, padded_input, list_input, array_input):
    for inputs in (ragged_input, padded_input, list_input, array_input):
        checker = get_data_checker(inputs)
        model = get_array_model()
        check_transform_produces_correct_output_type_forward(model, inputs, checker)


def test_with_list_forward(ragged_input, padded_input, list_input):
    for inputs in (ragged_input, padded_input, list_input):
        checker = get_data_checker(inputs)
        model = get_list_model()
        check_transform_produces_correct_output_type_forward(model, inputs, checker)


def test_with_padded_forward(ragged_input, padded_input, list_input, padded_data_input):
    for inputs in (ragged_input, padded_input, list_input, padded_data_input):
        checker = get_data_checker(inputs)
        model = get_padded_model()
        check_transform_produces_correct_output_type_forward(model, inputs, checker)


def test_with_ragged_forward(ragged_input, padded_input, list_input, ragged_data_input):
    for inputs in (ragged_input, padded_input, list_input, ragged_data_input):
        checker = get_data_checker(inputs)
        model = get_ragged_model()
        check_transform_produces_correct_output_type_forward(model, inputs, checker)


def test_with_array_backward(ragged_input, padded_input, list_input, array_input):
    for inputs in (ragged_input, padded_input, list_input, array_input):
        checker = get_data_checker(inputs)
        model = get_array_model()
        check_transform_produces_correct_output_type_backward(model, inputs, checker)


def test_with_list_backward(ragged_input, padded_input, list_input):
    for inputs in (ragged_input, padded_input, list_input):
        checker = get_data_checker(inputs)
        model = get_list_model()
        check_transform_produces_correct_output_type_backward(model, inputs, checker)


def test_with_ragged_backward(
    ragged_input, padded_input, list_input, ragged_data_input
):
    for inputs in (ragged_input, padded_input, list_input, ragged_data_input):
        checker = get_data_checker(inputs)
        model = get_ragged_model()
        check_transform_produces_correct_output_type_backward(model, inputs, checker)


def test_with_padded_backward(
    ragged_input, padded_input, list_input, padded_data_input
):
    for inputs in (ragged_input, padded_input, list_input, padded_data_input):
        checker = get_data_checker(inputs)
        model = get_padded_model()
        check_transform_produces_correct_output_type_backward(model, inputs, checker)


def test_with_getitem():
    data = (
        numpy.asarray([[1, 2, 3, 4]], dtype="f"),
        numpy.asarray([[5, 6, 7, 8]], dtype="f"),
    )
    model = with_getitem(1, Linear())
    model.initialize(data, data)
    Y, backprop = model.begin_update(data)
    assert len(Y) == len(data)
    assert numpy.array_equal(Y[0], data[0])  # the other item stayed the same
    assert not numpy.array_equal(Y[1], data[1])
    dX = backprop(Y)
    assert numpy.array_equal(dX[0], data[0])
    assert not numpy.array_equal(dX[1], data[1])
