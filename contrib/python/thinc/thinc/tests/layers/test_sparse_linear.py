import math

import numpy
import pytest

from thinc.api import SGD, SparseLinear, SparseLinear_v2, to_categorical


@pytest.fixture
def instances():
    lengths = numpy.asarray([5, 4], dtype="int32")
    keys = numpy.arange(9, dtype="uint64")
    values = numpy.ones(9, dtype="float32")
    X = (keys, values, lengths)
    y = numpy.asarray([0, 2], dtype="int32")
    return X, to_categorical(y, n_classes=3)


@pytest.fixture
def sgd():
    return SGD(0.001)


def test_basic(instances, sgd):
    X, y = instances
    nr_class = 3
    model = SparseLinear(nr_class).initialize()
    yh, backprop = model.begin_update(X)
    loss1 = ((yh - y) ** 2).sum()
    backprop(yh - y)
    model.finish_update(sgd)
    yh, backprop = model.begin_update(X)
    loss2 = ((yh - y) ** 2).sum()
    assert loss2 < loss1


def test_init():
    model = SparseLinear(3).initialize()
    keys = numpy.ones((5,), dtype="uint64")
    values = numpy.ones((5,), dtype="f")
    lengths = numpy.zeros((2,), dtype="int32")
    lengths[0] = 3
    lengths[1] = 2
    scores, backprop = model.begin_update((keys, values, lengths))
    assert scores.shape == (2, 3)
    d_feats = backprop(scores)
    assert len(d_feats) == 3


def test_distribution():
    n_class = 10
    length = 2**18
    model = SparseLinear_v2(nO=n_class, length=length).initialize()

    ii64 = numpy.iinfo(numpy.uint64)
    lengths = numpy.zeros((2,), dtype="int32")

    for p_nonzero in range(1, 12):
        # Clear gradients from the previous iterarion.
        model.set_grad("W", 0.0)

        n = 2**p_nonzero
        keys = numpy.random.randint(ii64.min, ii64.max, size=(n,), dtype=numpy.uint64)
        values = numpy.ones((n,), dtype="f")
        lengths[0] = n // 2
        lengths[1] = n // 2

        # Probability that a bit is set (2 because we use 2 hashes).
        p_nonzero = 1 - math.exp(-2 * n / length)

        Y, backprop = model.begin_update((keys, values, lengths))
        backprop(numpy.ones_like(Y))

        # Check that for each class we have the expected rate of non-zeros.
        dW = model.get_grad("W").reshape(n_class, -1)
        nonzero_empirical = numpy.count_nonzero(dW, axis=1) / dW.shape[1]
        numpy.testing.assert_allclose(
            nonzero_empirical, p_nonzero, rtol=1e-4, atol=1e-4
        )
