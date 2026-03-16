from typing import Tuple, cast

import numpy
import pytest
from numpy.testing import assert_allclose

from thinc.api import Model, NumpyOps, Softmax_v2
from thinc.types import Floats2d, Ints1d
from thinc.util import has_torch, torch2xp, xp2torch

OPS = NumpyOps()

inputs = OPS.xp.asarray([[4, 2, 3, 4], [1, 5, 3, 1], [9, 8, 5, 7]], dtype="f")
outputs = OPS.xp.asarray(
    [
        [0.39948627, 0.05406459, 0.14696279, 0.39948627],
        [0.01562812, 0.8532666, 0.11547707, 0.01562812],
        [0.657233, 0.24178252, 0.01203764, 0.08894681],
    ],
    dtype="f",
)


def test_unnormalized_softmax_backprop():
    model = Softmax_v2(normalize_outputs=False)
    model.initialize(inputs, outputs)
    _, backprop = model(inputs, is_train=False)
    with pytest.raises(ValueError, match="backprop is not supported"):
        backprop(OPS.xp.zeros_like(outputs))

    # Backprop should not fail when training.
    _, backprop = model(inputs, is_train=True)
    dX = backprop(OPS.xp.zeros_like(outputs))
    assert OPS.xp.all(dX == 0.0)


def torch_softmax_with_temperature(
    model: Model, X: Floats2d, targets: Ints1d
) -> Tuple[Floats2d, Floats2d]:
    import torch

    Wt = xp2torch(model.get_param("W"))
    bt = xp2torch(model.get_param("b"))
    temperature = model.attrs["softmax_temperature"]

    Xt = xp2torch(X, requires_grad=True)
    Yt_gold = xp2torch(targets).long()

    XWbt = (Xt @ Wt) + bt
    XWbt_temp = XWbt / temperature

    loss = torch.nn.CrossEntropyLoss()
    output = loss(XWbt_temp, Yt_gold)
    output.backward()

    return cast(
        Floats2d, torch2xp(torch.nn.functional.softmax(XWbt_temp, dim=-1))
    ), cast(Floats2d, torch2xp(cast(torch.Tensor, Xt.grad)))


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize("temperature", [0.5, 1.0, 2.0])
def test_softmax_temperature(temperature):
    model = Softmax_v2(
        temperature=temperature,
        init_W=lambda ops, shape: ops.xp.eye(shape[1], dtype="f"),
        init_b=lambda ops, shape: ops.xp.zeros(shape, dtype="f"),
    )

    X = OPS.xp.arange(-1, 1, 0.2, dtype="f").reshape(1, 10)
    targets = OPS.asarray1i([4])
    Y_gold = OPS.xp.eye(10, dtype="f")[targets]
    model.initialize(X, Y_gold)

    Yt, dXt = torch_softmax_with_temperature(model, X, targets)

    Y, backprop = model(X, is_train=True)
    dX = backprop(Y - Y_gold)

    assert_allclose(Y, Yt, atol=1e-4)
    assert_allclose(dX, dXt, atol=1e-4)


def test_reject_incorrect_temperature():
    with pytest.raises(ValueError, match=r"softmax temperature.*zero"):
        Softmax_v2(normalize_outputs=False, temperature=0.0)

    model = Softmax_v2(normalize_outputs=False)
    model.attrs["softmax_temperature"] = 0.0
    model.initialize(inputs, outputs)
    with pytest.raises(ValueError, match=r"softmax temperature.*zero"):
        model(inputs, is_train=False)
