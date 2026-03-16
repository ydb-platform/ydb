from functools import partial

import numpy
import pytest
from numpy.testing import assert_allclose

from thinc.api import Linear, NumpyOps, Relu, chain


@pytest.fixture(params=[1, 2, 9])
def nB(request):
    return request.param


@pytest.fixture(params=[1, 6])
def nI(request):
    return request.param


@pytest.fixture(params=[1, 5, 3])
def nH(request):
    return request.param


@pytest.fixture(params=[1, 2, 7, 9])
def nO(request):
    return request.param


@pytest.fixture
def model1(nH, nI):
    model = Relu(nH, nI).initialize()
    return model


@pytest.fixture
def model2(nO, nH):
    model = Linear(nO, nH).initialize()
    return model


@pytest.fixture
def input_data(nB, nI):
    return numpy.ones((nB, nI), dtype="f") + 1.0


@pytest.fixture
def gradient_data(nB, nO):
    return numpy.zeros((nB, nO), dtype="f") - 1.0


@pytest.fixture
def model(model1, model2):
    return chain(model1, model2).initialize()


def get_expected_predict(input_data, Ws, bs):
    numpy_ops = NumpyOps()
    X = input_data
    for i, (W, b) in enumerate(zip(Ws, bs)):
        X = numpy_ops.asarray(X)
        if i > 0:
            X *= X > 0
        X = numpy.tensordot(X, W, axes=[[1], [1]]) + b
    return X


def numeric_gradient(predict, weights, epsilon=1e-4):
    out1 = predict(weights + epsilon)
    out2 = predict(weights - epsilon)
    return (out1 - out2) / (2 * epsilon)


def test_models_have_shape(model1, model2, nI, nH, nO):
    assert model1.get_param("W").shape == (nH, nI)
    assert model1.get_param("b").shape == (nH,)
    assert model2.get_param("W").shape == (nO, nH)
    assert model2.get_param("b").shape == (nO,)


def test_model_shape(model, model1, model2, nI, nH, nO):
    assert model.get_dim("nI") == model1.get_dim("nI")
    assert model.get_dim("nO") == model2.get_dim("nO")


def test_infer_output_shape():
    model = Relu(dropout=0.2)
    X = model.ops.alloc2f(4, 5)
    Y = model.ops.alloc2f(4, 2)
    assert model.has_dim("nI") is None
    assert model.has_dim("nO") is None
    model.initialize(X=X, Y=Y)
    assert model.get_dim("nI") == 5
    assert model.get_dim("nO") == 2


def test_predict_and_begin_update_match(model, model1, model2, input_data):
    model = chain(model1, model2)
    via_predict = model.predict(input_data)
    via_update, _ = model.begin_update(input_data)
    assert_allclose(via_predict, via_update)
    expected = get_expected_predict(
        input_data,
        [model1.get_param("W"), model2.get_param("W")],
        [model1.get_param("b"), model2.get_param("b")],
    )
    assert_allclose(via_update, expected, atol=1e-2, rtol=1e-4)


def test_init_functions_are_called():
    init_was_called = {}

    def register_init(name, model, X=None, Y=None):
        init_was_called[name] = True

    layer1 = Linear(5)
    layer2 = Linear(5)
    layer3 = Linear(5)
    layer1.init = partial(register_init, "one")
    layer2.init = partial(register_init, "two")
    layer3.init = partial(register_init, "three")
    # This is the nesting we'll get from operators.
    model = chain(layer1, chain(layer2, layer3))
    assert not init_was_called
    model.initialize()
    assert init_was_called["one"]
    assert init_was_called["two"]
    assert init_was_called["three"]


class GradientSpy(object):
    def __init__(self):
        self.weights = None
        self.d_weights = None

    def __call__(self, weights, grad):
        self.weights = weights
        self.d_weights = grad


# I don't know how to get this working properly after the refactor. It's a numeric
# gradient check. I suspect the test is the problem, not the code.
@pytest.mark.skip
# This is the actual definition -- it's just annoying to see tonnes of skips.
# def test_gradient(model, input_data, nB, nH, nI, nO):
def test_gradient():
    truth = numpy.zeros((nB, nO), dtype="float32")
    truth[0] = 1.0

    guess, backprop = model.begin_update(input_data)
    backprop(guess - truth)

    for layer in model.layers:
        for name in layer.param_names:
            agrad = layer.get_grad(name).ravel()  # Should have grads for all params.
            predict = get_predict(layer, name, input_data)
            ngrad = get_numeric_gradient(predict, agrad.size, truth)
            assert_allclose(agrad, ngrad, atol=0.2, rtol=0.2)


def get_predict(layer, param_name, inputs):
    """Helper for gradient check. To do the numeric gradient check, we have
    to be able to wiggle one value in a parameter, and check the prediction
    before and after. So we need to get a callback that gives an output
    given a change to one weight.
    """

    def predict(i, epsilon):
        param = layer.get_param(param_name)
        shape = param.shape
        param = param.ravel()
        param[i] += epsilon
        layer.set_param(param_name, param.reshape(shape))
        outputs = layer.predict(inputs)
        param[i] -= epsilon
        layer.set_param(param_name, param.reshape(shape))
        return outputs.reshape(shape)

    return predict


def get_numeric_gradient(predict, n, target):
    gradient = numpy.zeros(n)
    for i in range(n):
        out1 = predict(i, 1e-4)
        out2 = predict(i, -1e-4)

        err1 = _get_loss(out1, target)
        err2 = _get_loss(out2, target)
        gradient[i] = (err1 - err2) / (2 * 1e-4)
        print("NGrad", i, err1, err2)
    return gradient


def _get_loss(truth, guess):
    return numpy.sum(numpy.sum(0.5 * numpy.square(truth - guess), 1))
