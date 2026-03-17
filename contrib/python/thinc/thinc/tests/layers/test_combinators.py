import numpy
import pytest
from numpy.testing import assert_allclose

from thinc.api import (
    Dropout,
    Linear,
    Model,
    NumpyOps,
    add,
    clone,
    concatenate,
    map_list,
    noop,
)
from thinc.layers import chain, tuplify


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
    return Linear(nH, nI)


@pytest.fixture
def model2(nO, nH):
    return Linear(nO, nH)


@pytest.fixture
def model3(nO):
    return Linear(nO, nO)


def test_tuplify_zero():
    with pytest.raises(TypeError):
        tuplify()


def test_tuplify_one(model1):
    with pytest.raises(TypeError):
        tuplify(model1)


def test_tuplify_two(model1, model2):
    model = tuplify(model1, model2)
    assert len(model.layers) == 2


def test_tuplify_operator_two(model1, model2):
    with Model.define_operators({"&": tuplify}):
        model = model1 & model2
        assert len(model.layers) == 2


def test_tuplify_dulicates_input():
    model = tuplify(noop(), noop())
    ones = numpy.ones([10])
    out = model.predict(ones)
    assert out == (ones, ones)


def test_tuplify_initialize(nI, nO):
    linear = Linear(nO)
    model = tuplify(linear, linear)
    ones = numpy.ones((1, nI), dtype="float")
    model.initialize(X=ones)


def test_tuplify_three(model1, model2, model3):
    model = tuplify(model1, model2, model3)
    assert len(model.layers) == 3


def test_tuplify_operator_three(model1, model2, model3):
    # Previously we 'flattened' these nested calls. We might opt to do so
    # again, especially for the operators.
    with Model.define_operators({"&": tuplify}):
        model = model1 & model2 & model3
        assert len(model.layers) == 2
        assert len(model.layers[0].layers) == 2


def test_chain_zero():
    with pytest.raises(TypeError):
        chain()


def test_chain_one(model1):
    with pytest.raises(TypeError):
        chain(model1)


def test_chain_two(model1, model2):
    model = chain(model1, model2)
    assert len(model.layers) == 2


def test_chain_operator_two(model1, model2):
    with Model.define_operators({">>": chain}):
        model = model1 >> model2
        assert len(model.layers) == 2


def test_chain_three(model1, model2, model3):
    model = chain(model1, model2, model3)
    assert len(model.layers) == 3


def test_chain_operator_three(model1, model2, model3):
    # Previously we 'flattened' these nested calls. We might opt to do so
    # again, especially for the operators.
    with Model.define_operators({">>": chain}):
        model = model1 >> model2 >> model3
        assert len(model.layers) == 2
        assert len(model.layers[0].layers) == 2


def test_chain_right_branch(model1, model2, model3):
    # Previously we 'flattened' these nested calls. We might opt to do so
    # again, especially for the operators.
    merge1 = chain(model1, model2)
    merge2 = chain(merge1, model3)
    assert len(merge1.layers) == 2
    assert len(merge2.layers) == 2


@pytest.mark.parametrize("ops", [NumpyOps(), NumpyOps(use_blis=True)])
def test_chain(ops):
    data = numpy.asarray([[1, 2, 3, 4]], dtype="f")
    model = chain(Linear(1), Dropout(), Linear(1))
    model.ops = ops
    model.initialize(data, data)
    Y, backprop = model(data, is_train=True)
    backprop(Y)
    # Layers with and without nO/nI
    model = chain(Linear(1), Dropout(), Linear(1, 1))
    model.initialize(data, data)
    # Setting dim on model
    model = chain(Linear(1), Dropout(), Linear(1))
    model.set_dim("nO", 1)
    model.initialize(data, None)
    model = chain(Linear(1, 1), Dropout(), Linear(1, 1))
    model.set_dim("nI", 1)
    model.initialize(None, data)
    # Not enough arguments
    with pytest.raises(TypeError):
        chain(Linear())
    with pytest.raises(TypeError):
        chain()


def test_concatenate_one(model1):
    model = concatenate(model1)
    assert isinstance(model, Model)


def test_concatenate_two(model1, model2):
    model = concatenate(model1, model2)
    assert len(model.layers) == 2


def test_concatenate_operator_two(model1, model2):
    with Model.define_operators({"|": concatenate}):
        model = model1 | model2
        assert len(model.layers) == 2


def test_concatenate_three(model1, model2, model3):
    model = concatenate(model1, model2, model3)
    assert len(model.layers) == 3


def test_concatenate_operator_three(model1, model2, model3):
    with Model.define_operators({"|": concatenate}):
        model = model1 | model2 | model3
        assert len(model.layers) == 3


def test_clone_changes_predictions(nH, nI):
    model1 = Linear(nH)
    model = clone(model1, 10)
    ones = numpy.ones((10, nI), dtype="f")
    model.initialize(X=ones)
    output_from_cloned = model.predict(ones)
    output_from_orig = model1.predict(ones)
    assert output_from_cloned.sum() != output_from_orig.sum()


def test_clone_gives_distinct_ids(nH, nI):
    model = clone(Linear(nH), 5)
    assert len(model.layers) == 5
    seen_ids = set()
    for node in model.walk():
        assert node.id not in seen_ids
        seen_ids.add(node.id)
    assert len(seen_ids) == 6


def test_clone_noop():
    model = clone(Linear(), 0)
    assert len(model.layers) == 0
    assert model.name == "noop"


def test_concatenate_noop():
    model = concatenate()
    assert len(model.layers) == 0
    assert model.name == "noop"


def test_noop():
    data = numpy.asarray([1, 2, 3], dtype="f")
    model = noop(Linear(), Linear())
    model.initialize(data, data)
    Y, backprop = model(data, is_train=True)
    assert numpy.array_equal(Y, data)
    dX = backprop(Y)
    assert numpy.array_equal(dX, data)


def test_add():
    data = numpy.asarray([[1, 2, 3, 4]], dtype="f")
    model = add(Linear(), Linear())
    model.initialize(data, data)
    Y, backprop = model(data, is_train=True)
    Y2 = sum(layer.predict(data) for layer in model.layers)
    assert numpy.array_equal(Y, Y2)
    dX = backprop(Y)
    assert dX.shape == data.shape
    # Test that nesting works
    model2 = add(model, Linear())
    assert len(model2.layers) == 3
    model.initialize(data, data)
    Y = model2.predict(data)
    Y2 = sum(layer.predict(data) for layer in model2.layers)
    assert numpy.array_equal(Y, Y2)


def test_add_edge_cases():
    data = numpy.asarray([[1, 2, 3, 4]], dtype="f")
    with pytest.raises(TypeError):
        add()
    model = add(Linear(), Linear())
    model._layers = []
    Y, backprop = model(data, is_train=True)
    assert numpy.array_equal(data, Y)
    dX = backprop(Y)
    assert numpy.array_equal(dX, data)


def test_concatenate():
    data = numpy.asarray([[1, 2, 3], [4, 5, 6]], dtype="f")
    model = concatenate(Linear(), Linear())
    model.initialize(data, data)
    Y, backprop = model(data, is_train=True)
    assert Y.shape[1] == sum([layer.predict(data).shape[1] for layer in model.layers])
    dX = backprop(Y)
    assert dX.shape == data.shape


def test_map_list():
    nI = 4
    nO = 9
    Xs = [numpy.zeros((6, nI), dtype="f"), numpy.ones((3, nI), dtype="f")]
    Y_shapes = [(x.shape[0], nO) for x in Xs]
    model = map_list(Linear())
    model.initialize(X=Xs, Y=[numpy.zeros(shape, dtype="f") for shape in Y_shapes])
    Ys, backprop = model(Xs, is_train=True)
    assert isinstance(Ys, list)
    assert len(Ys) == len(Xs)
    layer = model.layers[0]
    for X, Y in zip(Xs, Ys):
        assert_allclose(layer.predict(X), Y)
    dXs = backprop(Ys)
    assert isinstance(dXs, list)
    assert len(dXs) == len(Xs)
    assert dXs[0].shape == Xs[0].shape
    assert dXs[1].shape == Xs[1].shape
