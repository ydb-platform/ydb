import numpy
import pytest

from thinc.api import reduce_first, reduce_last, reduce_max, reduce_mean, reduce_sum
from thinc.types import Ragged


@pytest.fixture
def Xs():
    seqs = [numpy.zeros((10, 8), dtype="f"), numpy.zeros((4, 8), dtype="f")]
    for x in seqs:
        x[0] = 1
        x[1] = 2  # so max != first
        x[-1] = -1
    return seqs


def test_init_reduce_first():
    model = reduce_first()


def test_init_reduce_last():
    model = reduce_last()


def test_init_reduce_mean():
    model = reduce_mean()


def test_init_reduce_max():
    model = reduce_max()


def test_init_reduce_sum():
    model = reduce_sum()


def test_reduce_first(Xs):
    model = reduce_first()
    lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
    X = Ragged(model.ops.flatten(Xs), lengths)
    Y, backprop = model(X, is_train=True)
    assert isinstance(Y, numpy.ndarray)
    assert Y.shape == (len(Xs), Xs[0].shape[1])
    assert Y.dtype == Xs[0].dtype
    assert list(Y[0]) == list(Xs[0][0])
    assert list(Y[1]) == list(Xs[1][0])
    dX = backprop(Y)
    assert dX.dataXd.shape == X.dataXd.shape


def test_reduce_last(Xs):
    model = reduce_last()
    lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
    X = Ragged(model.ops.flatten(Xs), lengths)
    Y, backprop = model(X, is_train=True)
    assert isinstance(Y, numpy.ndarray)
    assert Y.shape == (len(Xs), Xs[0].shape[1])
    assert Y.dtype == Xs[0].dtype
    assert list(Y[0]) == list(Xs[0][-1])
    assert list(Y[1]) == list(Xs[1][-1])
    dX = backprop(Y)
    assert dX.dataXd.shape == X.dataXd.shape


def test_reduce_max(Xs):
    model = reduce_max()
    lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
    X = Ragged(model.ops.flatten(Xs), lengths)
    Y, backprop = model(X, is_train=True)
    assert isinstance(Y, numpy.ndarray)
    assert Y.shape == (len(Xs), Xs[0].shape[1])
    assert Y.dtype == Xs[0].dtype
    assert list(Y[0]) == list(Xs[0][1])
    assert list(Y[1]) == list(Xs[1][1])
    dX = backprop(Y)
    assert dX.dataXd.shape == X.dataXd.shape


def test_reduce_mean(Xs):
    Xs = [x * 1000 for x in Xs]  # use large numbers for numeric stability
    model = reduce_mean()
    lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
    X = Ragged(model.ops.flatten(Xs), lengths)
    Y, backprop = model(X, is_train=True)
    assert isinstance(Y, numpy.ndarray)
    assert Y.shape == (len(Xs), Xs[0].shape[1])
    assert Y.dtype == Xs[0].dtype
    assert numpy.all(Y[0] == Y[0][0])  # all values in row should be equal
    assert Y[0][0] == Xs[0].mean()
    assert numpy.all(Y[1] == Y[1][0])
    assert Y[1][0] == Xs[1].mean()
    dX = backprop(Y)
    assert dX.dataXd.shape == X.dataXd.shape


def test_reduce_sum(Xs):
    model = reduce_sum()
    lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
    X = Ragged(model.ops.flatten(Xs), lengths)
    Y, backprop = model(X, is_train=True)
    assert isinstance(Y, numpy.ndarray)
    assert Y.shape == (len(Xs), Xs[0].shape[1])
    assert Y.dtype == Xs[0].dtype
    assert Y[0][0] == Xs[0][:, 0].sum()
    assert numpy.all(Y[0] == Y[0][0])
    assert Y[1][-1] == Xs[1][:, 0].sum()
    assert numpy.all(Y[1] == Y[1][0])
    dX = backprop(Y)
    assert dX.dataXd.shape == X.dataXd.shape


def test_size_mismatch(Xs):
    for reduce in [reduce_first, reduce_last, reduce_max, reduce_mean, reduce_sum]:
        model = reduce()
        lengths = model.ops.asarray([x.shape[0] for x in Xs], dtype="i")
        X = Ragged(model.ops.flatten(Xs), lengths)
        Y, backprop = model(X, is_train=True)

        Y_bad = Y[:-1]
        with pytest.raises(ValueError):
            backprop(Y_bad)
