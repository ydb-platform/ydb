# Copyright ExplsionAI GmbH, released under BSD.
from __future__ import print_function

import numpy as np

np.random.seed(0)
from numpy.testing import assert_allclose

from hypothesis import assume
from hypothesis.strategies import tuples, integers, floats
from hypothesis.extra.numpy import arrays


def lengths(lo=1, hi=10):
    return integers(min_value=lo, max_value=hi)


def shapes(min_rows=1, max_rows=100, min_cols=1, max_cols=100):
    return tuples(lengths(lo=min_rows, hi=max_rows), lengths(lo=min_cols, hi=max_cols))


def ndarrays_of_shape(shape, lo=-1000.0, hi=1000.0, dtype="float64"):
    width = 64 if dtype == "float64" else 32
    return arrays(
        dtype, shape=shape, elements=floats(min_value=lo, max_value=hi, width=width)
    )


def ndarrays(
    min_len=0, max_len=10, min_val=-10000000.0, max_val=1000000.0, dtype="float64"
):
    return lengths(lo=min_len, hi=max_len).flatmap(
        lambda n: ndarrays_of_shape(n, lo=min_val, hi=max_val, dtype=dtype)
    )


def matrices(
    min_rows=1,
    max_rows=10,
    min_cols=1,
    max_cols=10,
    min_value=-10000000.0,
    max_value=1000000.0,
    dtype="float64",
):
    return shapes(
        min_rows=min_rows, max_rows=max_rows, min_cols=min_cols, max_cols=max_cols
    ).flatmap(lambda mn: ndarrays_of_shape(mn, lo=min_value, hi=max_value, dtype=dtype))


def positive_ndarrays(min_len=0, max_len=10, max_val=100000.0, dtype="float64"):
    return ndarrays(
        min_len=min_len, max_len=max_len, min_val=0, max_val=max_val, dtype=dtype
    )


def negative_ndarrays(min_len=0, max_len=10, min_val=-100000.0, dtype="float64"):
    return ndarrays(
        min_len=min_len, max_len=max_len, min_val=min_val, max_val=-1e-10, dtype=dtype
    )


def parse_layer(layer_data):
    # Get the first row, excluding the first column
    x = layer_data[0, 1:]
    # Get the first column, excluding the first row
    # .ascontiguousarray is support important here!!!!
    b = np.ascontiguousarray(layer_data[1:, 0], dtype="float64")
    # Slice out the row and the column used for the X and the bias
    W = layer_data[1:, 1:]
    assert x.ndim == 1
    assert b.ndim == 1
    assert b.shape[0] == W.shape[0]
    assert x.shape[0] == W.shape[1]
    assume(not np.isnan(W.sum()))
    assume(not np.isnan(x.sum()))
    assume(not np.isnan(b.sum()))
    assume(not any(np.isinf(val) for val in W.flatten()))
    assume(not any(np.isinf(val) for val in x))
    assume(not any(np.isinf(val) for val in b))
    return x, b, W


def split_row(layer_data):
    return (layer_data[0, :], layer_data[:, :])
