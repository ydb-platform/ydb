import numpy
import pytest
from hypothesis import given, settings
from mock import MagicMock
from numpy.testing import assert_allclose

from thinc.api import SGD, Dropout, Linear, chain

from ..strategies import arrays_OI_O_BI
from ..util import get_model, get_shape


@pytest.fixture
def model():
    model = Linear()
    return model


def test_linear_default_name(model):
    assert model.name == "linear"


def test_linear_dimensions_on_data():
    X = MagicMock(shape=(5, 10), spec=numpy.ndarray)
    X.ndim = 2
    X.dtype = "float32"
    y = MagicMock(shape=(8,), spec=numpy.ndarray)
    y.ndim = 2
    y.dtype = "float32"
    y.max = MagicMock()
    model = Linear()
    model.initialize(X, y)
    assert model.get_dim("nI") is not None
    y.max.assert_called_with()


@pytest.mark.skip(reason="Flaky, skip temporarily")
@given(arrays_OI_O_BI(max_batch=8, max_out=8, max_in=8))
def test_begin_update_matches_predict(W_b_input):
    model = get_model(W_b_input)
    nr_batch, nr_out, nr_in = get_shape(W_b_input)
    W, b, input_ = W_b_input
    fwd_via_begin_update, finish_update = model.begin_update(input_)
    fwd_via_predict_batch = model.predict(input_)
    assert_allclose(fwd_via_begin_update, fwd_via_predict_batch)


@pytest.mark.skip(reason="Flaky, skip temporarily")
@given(arrays_OI_O_BI(max_batch=8, max_out=8, max_in=8))
def test_finish_update_calls_optimizer_with_weights(W_b_input):
    model = get_model(W_b_input)
    nr_batch, nr_out, nr_in = get_shape(W_b_input)
    W, b, input_ = W_b_input
    output, finish_update = model.begin_update(input_)

    seen_keys = set()

    def sgd(key, data, gradient, **kwargs):
        seen_keys.add(key)
        assert data.shape == gradient.shape
        return data, gradient

    grad_BO = numpy.ones((nr_batch, nr_out), dtype="f")
    grad_BI = finish_update(grad_BO)  # noqa: F841
    model.finish_update(sgd)
    for name in model.param_names:
        assert (model.id, name) in seen_keys


@pytest.mark.skip(reason="Flaky, skip temporarily")
@settings(max_examples=100)
@given(arrays_OI_O_BI(max_batch=8, max_out=8, max_in=8))
def test_predict_small(W_b_input):
    W, b, input_ = W_b_input
    nr_out, nr_in = W.shape
    model = Linear(nr_out, nr_in)
    model.set_param("W", W)
    model.set_param("b", b)

    einsummed = numpy.einsum(
        "oi,bi->bo",
        numpy.asarray(W, dtype="float64"),
        numpy.asarray(input_, dtype="float64"),
        optimize=False,
    )

    expected_output = einsummed + b

    predicted_output = model.predict(input_)
    assert_allclose(predicted_output, expected_output, rtol=0.01, atol=0.01)


@pytest.mark.skip(reason="Flaky, skip temporarily")
@given(arrays_OI_O_BI(max_batch=20, max_out=30, max_in=30))
@settings(deadline=None)
def test_predict_extensive(W_b_input):
    W, b, input_ = W_b_input
    nr_out, nr_in = W.shape
    model = Linear(nr_out, nr_in)
    model.set_param("W", W)
    model.set_param("b", b)

    einsummed = numpy.einsum(
        "bi,oi->bo",
        numpy.asarray(input_, dtype="float32"),
        numpy.asarray(W, dtype="float32"),
        optimize=False,
    )

    expected_output = einsummed + b

    predicted_output = model.predict(input_)
    assert_allclose(predicted_output, expected_output, rtol=1e-04, atol=0.0001)


@pytest.mark.skip(reason="Flaky, skip temporarily")
@given(arrays_OI_O_BI(max_batch=8, max_out=8, max_in=8))
def test_dropout_gives_zero_activations(W_b_input):
    model = chain(get_model(W_b_input), Dropout(1.0))
    nr_batch, nr_out, nr_in = get_shape(W_b_input)
    W, b, input_ = W_b_input
    fwd_dropped, _ = model.begin_update(input_)
    assert all(val == 0.0 for val in fwd_dropped.flatten())


@pytest.mark.skip(reason="Flaky, skip temporarily")
@given(arrays_OI_O_BI(max_batch=8, max_out=8, max_in=8))
def test_dropout_gives_zero_gradients(W_b_input):
    model = chain(get_model(W_b_input), Dropout(1.0))
    nr_batch, nr_out, nr_in = get_shape(W_b_input)
    W, b, input_ = W_b_input
    for node in model.walk():
        if node.name == "dropout":
            node.attrs["dropout_rate"] = 1.0
    fwd_dropped, finish_update = model.begin_update(input_)
    grad_BO = numpy.ones((nr_batch, nr_out), dtype="f")
    grad_BI = finish_update(grad_BO)
    assert all(val == 0.0 for val in grad_BI.flatten())


@pytest.fixture
def model2():
    model = Linear(2, 2).initialize()
    return model


def test_init(model2):
    assert model2.get_dim("nO") == 2
    assert model2.get_dim("nI") == 2
    assert model2.get_param("W") is not None
    assert model2.get_param("b") is not None


def test_predict_bias(model2):
    input_ = model2.ops.alloc2f(1, model2.get_dim("nI"))
    target_scores = model2.ops.alloc2f(1, model2.get_dim("nI"))
    scores = model2.predict(input_)
    assert_allclose(scores[0], target_scores[0])
    # Set bias for class 0
    model2.get_param("b")[0] = 2.0
    target_scores[0, 0] = 2.0
    scores = model2.predict(input_)
    assert_allclose(scores, target_scores)
    # Set bias for class 1
    model2.get_param("b")[1] = 5.0
    target_scores[0, 1] = 5.0
    scores = model2.predict(input_)
    assert_allclose(scores, target_scores)


@pytest.mark.parametrize(
    "X,expected",
    [
        (numpy.asarray([0.0, 0.0], dtype="f"), [0.0, 0.0]),
        (numpy.asarray([1.0, 0.0], dtype="f"), [1.0, 0.0]),
        (numpy.asarray([0.0, 1.0], dtype="f"), [0.0, 1.0]),
        (numpy.asarray([1.0, 1.0], dtype="f"), [1.0, 1.0]),
    ],
)
def test_predict_weights(X, expected):
    W = numpy.asarray([1.0, 0.0, 0.0, 1.0], dtype="f").reshape((2, 2))
    bias = numpy.asarray([0.0, 0.0], dtype="f")

    model = Linear(W.shape[0], W.shape[1])
    model.set_param("W", W)
    model.set_param("b", bias)

    scores = model.predict(X.reshape((1, -1)))
    assert_allclose(scores.ravel(), expected)


def test_update():
    W = numpy.asarray([1.0, 0.0, 0.0, 1.0], dtype="f").reshape((2, 2))
    bias = numpy.asarray([0.0, 0.0], dtype="f")

    model = Linear(2, 2)
    model.set_param("W", W)
    model.set_param("b", bias)
    sgd = SGD(1.0, L2=0.0, grad_clip=0.0)
    sgd.averages = None

    ff = numpy.asarray([[0.0, 0.0]], dtype="f")
    tf = numpy.asarray([[1.0, 0.0]], dtype="f")
    ft = numpy.asarray([[0.0, 1.0]], dtype="f")  # noqa: F841
    tt = numpy.asarray([[1.0, 1.0]], dtype="f")  # noqa: F841

    # ff, i.e. 0, 0
    scores, backprop = model.begin_update(ff)
    assert_allclose(scores[0, 0], scores[0, 1])
    # Tell it the answer was 'f'
    gradient = numpy.asarray([[-1.0, 0.0]], dtype="f")
    backprop(gradient)
    for key, (param, d_param) in model.get_gradients().items():
        param, d_param = sgd(key, param, d_param)
        model.set_param(key[1], param)
        model.set_grad(key[1], d_param)

    b = model.get_param("b")
    W = model.get_param("W")
    assert b[0] == 1.0
    assert b[1] == 0.0
    # Unchanged -- input was zeros, so can't get gradient for weights.
    assert W[0, 0] == 1.0
    assert W[0, 1] == 0.0
    assert W[1, 0] == 0.0
    assert W[1, 1] == 1.0

    # tf, i.e. 1, 0
    scores, finish_update = model.begin_update(tf)
    # Tell it the answer was 'T'
    gradient = numpy.asarray([[0.0, -1.0]], dtype="f")
    finish_update(gradient)
    for key, (W, dW) in model.get_gradients().items():
        sgd(key, W, dW)
    b = model.get_param("b")
    W = model.get_param("W")
    assert b[0] == 1.0
    assert b[1] == 1.0
    # Gradient for weights should have been outer(gradient, input)
    # so outer([0, -1.], [1., 0.])
    # =  [[0., 0.], [-1., 0.]]
    assert W[0, 0] == 1.0 - 0.0
    assert W[0, 1] == 0.0 - 0.0
    assert W[1, 0] == 0.0 - -1.0
    assert W[1, 1] == 1.0 - 0.0
