import numpy
import pytest

from thinc.api import (
    SGD,
    ArgsKwargs,
    CupyOps,
    Linear,
    MPSOps,
    NumpyOps,
    PyTorchWrapper,
    PyTorchWrapper_v2,
    PyTorchWrapper_v3,
    Relu,
    chain,
    get_current_ops,
    torch2xp,
    use_ops,
    xp2torch,
)
from thinc.backends import context_pools
from thinc.compat import has_cupy_gpu, has_torch, has_torch_amp, has_torch_mps_gpu
from thinc.layers.pytorchwrapper import PyTorchWrapper_v3
from thinc.shims.pytorch import (
    default_deserialize_torch_model,
    default_serialize_torch_model,
)
from thinc.shims.pytorch_grad_scaler import PyTorchGradScaler
from thinc.util import get_torch_default_device

from ..util import check_input_converters, make_tempdir

XP_OPS = [NumpyOps()]
if has_cupy_gpu:
    XP_OPS.append(CupyOps())
if has_torch_mps_gpu:
    XP_OPS.append(MPSOps())


if has_torch_amp:
    TORCH_MIXED_PRECISION = [False, True]
else:
    TORCH_MIXED_PRECISION = [False]

XP_OPS_MIXED = [
    (ops, mixed)
    for ops in XP_OPS
    for mixed in TORCH_MIXED_PRECISION
    if not mixed or isinstance(ops, CupyOps)
]


def check_learns_zero_output(model, sgd, X, Y):
    """Check we can learn to output a zero vector"""
    Yh, get_dX = model.begin_update(X)
    dYh = (Yh - Y) / Yh.shape[0]
    dX = get_dX(dYh)
    model.finish_update(sgd)
    prev = numpy.abs(Yh.sum())
    for i in range(100):
        Yh, get_dX = model.begin_update(X)
        total = numpy.abs(Yh.sum())
        dX = get_dX(Yh - Y)  # noqa: F841
        model.finish_update(sgd)
    assert total < prev


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize("nN,nI,nO", [(2, 3, 4)])
def test_pytorch_unwrapped(nN, nI, nO):
    model = Linear(nO, nI).initialize()
    X = numpy.zeros((nN, nI), dtype="f")
    X += numpy.random.uniform(size=X.size).reshape(X.shape)
    sgd = SGD(0.01)
    Y = numpy.zeros((nN, nO), dtype="f")
    check_learns_zero_output(model, sgd, X, Y)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize("nN,nI,nO", [(2, 3, 4)])
def test_pytorch_wrapper(nN, nI, nO):
    import torch.nn

    model = PyTorchWrapper(torch.nn.Linear(nI, nO)).initialize()
    sgd = SGD(0.001)
    X = numpy.zeros((nN, nI), dtype="f")
    X += numpy.random.uniform(size=X.size).reshape(X.shape)
    Y = numpy.zeros((nN, nO), dtype="f")
    Yh, get_dX = model.begin_update(X)
    assert isinstance(Yh, numpy.ndarray)
    assert Yh.shape == (nN, nO)
    dYh = (Yh - Y) / Yh.shape[0]
    dX = get_dX(dYh)
    model.finish_update(sgd)
    assert dX.shape == (nN, nI)
    check_learns_zero_output(model, sgd, X, Y)
    assert isinstance(model.predict(X), numpy.ndarray)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize("ops_mixed", XP_OPS_MIXED)
@pytest.mark.parametrize("nN,nI,nO", [(2, 3, 4)])
def test_pytorch_wrapper_thinc_input(ops_mixed, nN, nI, nO):
    import torch.nn

    ops, mixed_precision = ops_mixed

    with use_ops(ops.name):
        ops = get_current_ops()
        pytorch_layer = torch.nn.Linear(nO, nO)
        # Initialize with large weights to trigger overflow of FP16 in
        # mixed-precision training.
        torch.nn.init.uniform_(pytorch_layer.weight, 9.0, 11.0)
        device = get_torch_default_device()
        model = chain(
            Relu(),
            PyTorchWrapper_v2(
                pytorch_layer.to(device),
                mixed_precision=mixed_precision,
                grad_scaler=PyTorchGradScaler(
                    enabled=mixed_precision, init_scale=2.0**16
                ),
            ).initialize(),
        )
        # pytorch allocator is set in PyTorchShim
        if isinstance(ops, CupyOps):
            assert "pytorch" in context_pools.get()
        sgd = SGD(0.001)
        X = ops.xp.zeros((nN, nI), dtype="f")
        X += ops.xp.random.uniform(size=X.size).reshape(X.shape)
        Y = ops.xp.zeros((nN, nO), dtype="f")
        model.initialize(X, Y)
        Yh, get_dX = model.begin_update(X)
        assert isinstance(Yh, ops.xp.ndarray)
        assert Yh.shape == (nN, nO)
        dYh = (Yh - Y) / Yh.shape[0]
        dX = get_dX(dYh)
        model.finish_update(sgd)
        assert dX.shape == (nN, nI)
        check_learns_zero_output(model, sgd, X, Y)
        assert isinstance(model.predict(X), ops.xp.ndarray)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
def test_pytorch_roundtrip_conversion():
    import torch

    xp_tensor = numpy.zeros((2, 3), dtype="f")
    torch_tensor = xp2torch(xp_tensor)
    assert isinstance(torch_tensor, torch.Tensor)
    new_xp_tensor = torch2xp(torch_tensor)
    assert numpy.array_equal(xp_tensor, new_xp_tensor)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
def test_pytorch_wrapper_roundtrip():
    import torch.nn

    model = PyTorchWrapper(torch.nn.Linear(2, 3))
    model_bytes = model.to_bytes()
    PyTorchWrapper(torch.nn.Linear(2, 3)).from_bytes(model_bytes)
    with make_tempdir() as path:
        model_path = path / "model"
        model.to_disk(model_path)
        new_model = PyTorchWrapper(torch.nn.Linear(2, 3)).from_bytes(model_bytes)
        new_model.from_disk(model_path)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.parametrize(
    "data,n_args,kwargs_keys",
    [
        # fmt: off
        (numpy.zeros((2, 3), dtype="f"), 1, []),
        ([numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")], 2, []),
        ((numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")), 2, []),
        ({"a": numpy.zeros((2, 3), dtype="f"), "b": numpy.zeros((2, 3), dtype="f")}, 0, ["a", "b"]),
        (ArgsKwargs((numpy.zeros((2, 3), dtype="f"), numpy.zeros((2, 3), dtype="f")), {"c": numpy.zeros((2, 3), dtype="f")}), 2, ["c"]),
        # fmt: on
    ],
)
def test_pytorch_convert_inputs(data, n_args, kwargs_keys):
    import torch.nn

    model = PyTorchWrapper(torch.nn.Linear(3, 4))
    convert_inputs = model.attrs["convert_inputs"]
    Y, backprop = convert_inputs(model, data, is_train=True)
    check_input_converters(Y, backprop, data, n_args, kwargs_keys, torch.Tensor)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
def test_pytorch_wrapper_custom_serde():
    import torch.nn

    def serialize(model):
        return default_serialize_torch_model(model)

    def deserialize(model, state_bytes, device):
        return default_deserialize_torch_model(model, state_bytes, device)

    def get_model():
        return PyTorchWrapper_v3(
            torch.nn.Linear(2, 3),
            serialize_model=serialize,
            deserialize_model=deserialize,
        )

    model = get_model()
    model_bytes = model.to_bytes()
    get_model().from_bytes(model_bytes)
    with make_tempdir() as path:
        model_path = path / "model"
        model.to_disk(model_path)
        new_model = get_model().from_bytes(model_bytes)
        new_model.from_disk(model_path)
