from typing import List, Optional

import numpy
import pytest
import srsly
from numpy.testing import assert_almost_equal

from thinc.api import Dropout, Model, NumpyOps, registry, with_padded
from thinc.backends import NumpyOps
from thinc.compat import has_torch
from thinc.types import Array2d, Floats2d, FloatsXd, Padded, Ragged, Shape
from thinc.util import data_validation, get_width

OPS = NumpyOps()


class NoDropoutOps(NumpyOps):
    def get_dropout_mask(self, shape: Shape, drop: Optional[float]) -> FloatsXd:
        if drop is None or drop <= 0:
            return self.xp.ones(shape, dtype="f")
        else:
            raise ValueError("During prediction, dropout should not be applied")


array1d = OPS.xp.asarray([1, 2, 3], dtype="f")
array1dint = OPS.xp.asarray([1, 2, 3], dtype="i")
array2d = OPS.xp.asarray([[4, 2, 3, 4], [1, 5, 3, 1], [9, 8, 5, 7]], dtype="f")
array2dint = OPS.xp.asarray([[1, 2, 3], [4, 5, 6]], dtype="i")
array3d = OPS.xp.zeros((3, 3, 3), dtype="f")
ragged = Ragged(array2d, OPS.xp.asarray([2, 1], dtype="i"))
padded = Padded(
    array3d, array1d, OPS.asarray1i([1, 2, 3, 4]), OPS.asarray1i([1, 2, 3, 4])
)
width = array2d.shape[1]
vectors = numpy.zeros((array2dint.max(), 1), dtype="f")


def assert_data_match(Y, out_data):
    assert type(Y) == type(out_data)
    if isinstance(out_data, OPS.xp.ndarray):
        assert isinstance(Y, OPS.xp.ndarray)
        assert out_data.ndim == Y.ndim
    elif isinstance(out_data, Ragged):
        assert isinstance(Y, Ragged)
        assert out_data.data.ndim == Y.data.ndim
        assert out_data.lengths.ndim == Y.lengths.ndim
    elif isinstance(out_data, Padded):
        assert isinstance(Y, Padded)
        assert out_data.data.ndim == Y.data.ndim
        assert out_data.size_at_t.ndim == Y.size_at_t.ndim
        assert len(out_data.lengths) == len(Y.lengths)
        assert len(out_data.indices) == len(Y.indices)
    elif isinstance(out_data, (list, tuple)):
        assert isinstance(Y, (list, tuple))
        assert all(isinstance(x, numpy.ndarray) for x in Y)
    else:
        pytest.fail(f"wrong output of {type(Y)}: {Y}")


TEST_CASES_SUMMABLE = [
    # Array to array
    ("Dish.v1", {}, array2d, array2d),
    ("Dish.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Dropout.v1", {}, array2d, array2d),
    ("LayerNorm.v1", {}, array2d, array2d),
    ("Linear.v1", {}, array2d, array2d),
    ("Logistic.v1", {}, array2d, array2d),
    ("Maxout.v1", {}, array2d, array2d),
    ("Maxout.v1", {"normalize": True, "dropout": 0.2}, array2d, array2d),
    ("Maxout.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Mish.v1", {}, array2d, array2d),
    ("Mish.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Mish.v1", {"normalize": True, "dropout": 0.2}, array2d, array2d),
    ("Relu.v1", {}, array2d, array2d),
    ("Relu.v1", {"normalize": True, "dropout": 0.2}, array2d, array2d),
    ("Sigmoid.v1", {}, array2d, array2d),
    ("Sigmoid.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("ClippedLinear.v1", {}, array2d, array2d),
    ("ClippedLinear.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("ReluK.v1", {}, array2d, array2d),
    ("ReluK.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("HardSigmoid.v1", {}, array2d, array2d),
    ("HardSigmoid.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("HardTanh.v1", {}, array2d, array2d),
    ("HardTanh.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("HardSwish.v1", {}, array2d, array2d),
    ("HardSwish.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("HardSwishMobilenet.v1", {}, array2d, array2d),
    ("HardSwishMobilenet.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Swish.v1", {}, array2d, array2d),
    ("Swish.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Gelu.v1", {}, array2d, array2d),
    ("Gelu.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("sigmoid_activation.v1", {}, array2d, array2d),
    ("softmax_activation.v1", {}, array2d, array2d),
    ("Softmax.v1", {}, array2d, array2d),
    ("Softmax.v1", {"nO": 4, "nI": 4}, array2d, array2d),
    ("Softmax.v2", {}, array2d, array2d),
    ("Softmax.v2", {"nO": 4, "nI": 4}, array2d, array2d),
    # fmt: off
    # List to list
    ("LSTM.v1", {"bi": False}, [array2d, array2d], [array2d, array2d]),
    pytest.param("PyTorchLSTM.v1", {"bi": False, "nO": width, "nI": width}, [array2d, array2d], [array2d, array2d], marks=pytest.mark.skipif(not has_torch, reason="needs PyTorch")),
    # fmt: on
]

TEST_CASES = [
    *TEST_CASES_SUMMABLE,
    pytest.param(
        "PyTorchLSTM.v1",
        {"bi": True, "nO": width * 2, "nI": width},
        [array2d, array2d],
        [array2d, array2d],
        marks=pytest.mark.skipif(not has_torch, reason="needs PyTorch"),
    ),
    ("LSTM.v1", {"bi": True}, [array2d, array2d], [array2d, array2d]),
    # Ragged to array
    ("reduce_max.v1", {}, ragged, array2d),
    ("reduce_mean.v1", {}, ragged, array2d),
    ("reduce_sum.v1", {}, ragged, array2d),
    # fmt: off
    # Other
    ("expand_window.v1", {}, array2d, array2d),
    ("expand_window.v1", {}, ragged, ragged),
    ("Embed.v1", {"nO": 4, "nV": int(array2dint.max() + 1), "column": 0, "dropout": 0.2}, array2dint, array2d),
    ("Embed.v1", {"nO": 4, "nV": int(array1dint.max() + 1)}, array1dint, array2d),
    ("HashEmbed.v1", {"nO": 1, "nV": int(array2dint.max()), "column": 0, "dropout": 0.2}, array2dint, array2d),
    ("HashEmbed.v1", {"nO": 1, "nV": 2}, array1dint, array2d),
    ("MultiSoftmax.v1", {"nOs": (1, 3)}, array2d, array2d),
    # ("CauchySimilarity.v1", {}, (array2d, array2d), array1d),
    ("ParametricAttention.v1", {}, ragged, ragged),
    ("ParametricAttention.v2", {}, ragged, ragged),
    ("ParametricAttention.v2", {"key_transform": {"@layers": "Gelu.v1"}}, ragged, ragged),
    ("SparseLinear.v1", {}, (numpy.asarray([1, 2, 3], dtype="uint64"), array1d, numpy.asarray([1, 1], dtype="i")), array2d),
    ("SparseLinear.v2", {}, (numpy.asarray([1, 2, 3], dtype="uint64"), array1d, numpy.asarray([1, 1], dtype="i")), array2d),
    ("remap_ids.v1", {"dtype": "f"}, ["a", 1, 5.0], array2dint),
    ("remap_ids.v2", {"mapping_table": {}, "column": 1}, numpy.array([[1, 2, 3], [4, 5, 6]]).T, array2dint),
    ("premap_ids.v1", {"mapping_table": {}, "column": 1}, numpy.array([[1, 2, 3], [4, 5, 6]]).T, array2dint),
    # fmt: on
]


@pytest.mark.parametrize("name,kwargs,in_data,out_data", TEST_CASES)
def test_layers_from_config(name, kwargs, in_data, out_data):
    cfg = {"@layers": name, **kwargs}
    filled_cfg = registry.fill({"config": cfg})
    assert srsly.is_json_serializable(filled_cfg)
    model = registry.resolve({"config": cfg})["config"]
    if "LSTM" in name:
        model = with_padded(model)
    valid = True
    with data_validation(valid):
        model.initialize(in_data, out_data)
        Y, backprop = model(in_data, is_train=True)
        if model.has_dim("nO"):
            assert get_width(Y) == model.get_dim("nO")
        assert_data_match(Y, out_data)
        dX = backprop(Y)
        assert_data_match(dX, in_data)
        # Test that during predictions, no dropout is applied
        model._to_ops(NoDropoutOps())
        model.predict(in_data)


@pytest.mark.parametrize("name,kwargs,in_data,out_data", TEST_CASES_SUMMABLE)
def test_layers_with_residual(name, kwargs, in_data, out_data):
    cfg = {"@layers": "residual.v1", "layer": {"@layers": name, **kwargs}}
    model = registry.resolve({"config": cfg})["config"]
    if "LSTM" in name:
        model = with_padded(model)
    model.initialize(in_data, out_data)
    Y, backprop = model(in_data, is_train=True)
    assert_data_match(Y, out_data)
    dX = backprop(Y)
    assert_data_match(dX, in_data)


@pytest.mark.parametrize("data", [array2d, ragged, padded, [array2d, array2d]])
def test_dropout(data):
    model = Dropout(0.2)
    model.initialize(data, data)
    Y, backprop = model(data, is_train=False)
    assert_data_match(Y, data)
    dX = backprop(Y)
    assert_data_match(dX, data)


@pytest.mark.parametrize("name,kwargs,in_data,out_data", TEST_CASES)
def test_layers_batching_all(name, kwargs, in_data, out_data):
    cfg = {"@layers": name, **kwargs}
    model = registry.resolve({"config": cfg})["config"]
    if "expand_window" in name:
        return
    if "LSTM" in name:
        model = with_padded(model)
        util_batch_unbatch_list(model, in_data, out_data)
    else:
        if isinstance(in_data, OPS.xp.ndarray) and in_data.ndim == 2:
            if isinstance(out_data, OPS.xp.ndarray) and out_data.ndim == 2:
                util_batch_unbatch_array(model, in_data, out_data)
        if isinstance(in_data, Ragged):
            if isinstance(out_data, OPS.xp.ndarray) and out_data.ndim == 2:
                util_batch_unbatch_ragged(model, in_data, out_data)


def util_batch_unbatch_array(
    model: Model[Floats2d, Array2d], in_data: Floats2d, out_data: Array2d
):
    unbatched = [model.ops.reshape2f(a, 1, -1) for a in in_data]
    with data_validation(True):
        model.initialize(in_data, out_data)
        Y_batched = model.predict(in_data).tolist()
        Y_not_batched = [model.predict(u)[0].tolist() for u in unbatched]
        assert_almost_equal(Y_batched, Y_not_batched, decimal=4)


def util_batch_unbatch_list(
    model: Model[List[Array2d], List[Array2d]],
    in_data: List[Array2d],
    out_data: List[Array2d],
):
    with data_validation(True):
        model.initialize(in_data, out_data)
        Y_batched = model.predict(in_data)
        Y_not_batched = [model.predict([u])[0] for u in in_data]
        assert_almost_equal(Y_batched, Y_not_batched, decimal=4)  # type: ignore


def util_batch_unbatch_ragged(
    model: Model[Ragged, Array2d], in_data: Ragged, out_data: Array2d
):
    with data_validation(True):
        model.initialize(in_data, out_data)
        Y_batched = model.predict(in_data)
        Y_not_batched = [model.predict(in_data[i])[0] for i in range(len(in_data))]
        assert_almost_equal(Y_batched, Y_not_batched, decimal=4)  # type: ignore
