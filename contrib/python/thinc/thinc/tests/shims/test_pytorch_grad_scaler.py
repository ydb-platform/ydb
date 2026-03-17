import pytest
from hypothesis import given, settings
from hypothesis.strategies import lists, one_of, tuples

from thinc.api import PyTorchGradScaler
from thinc.compat import has_torch, has_torch_amp, has_torch_cuda_gpu, torch
from thinc.util import is_torch_array

from ..strategies import ndarrays


def tensors():
    return ndarrays().map(lambda a: torch.tensor(a).cuda())


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.skipif(not has_torch_cuda_gpu, reason="needs a GPU")
@pytest.mark.skipif(
    not has_torch_amp, reason="requires PyTorch with mixed-precision support"
)
@given(X=one_of(tensors(), lists(tensors()), tuples(tensors())))
@settings(deadline=None)
def test_scale_random_inputs(X):
    import torch

    device_id = torch.cuda.current_device()
    scaler = PyTorchGradScaler(enabled=True)
    scaler.to_(device_id)

    if is_torch_array(X):
        assert torch.allclose(scaler.scale(X), X * 2.0**16)
    else:
        scaled1 = scaler.scale(X)
        scaled2 = [t * 2.0**16 for t in X]
        for t1, t2 in zip(scaled1, scaled2):
            assert torch.allclose(t1, t2)


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.skipif(not has_torch_cuda_gpu, reason="needs a GPU")
@pytest.mark.skipif(
    not has_torch_amp, reason="requires PyTorch with mixed-precision support"
)
def test_grad_scaler():
    import torch

    device_id = torch.cuda.current_device()

    scaler = PyTorchGradScaler(enabled=True)
    scaler.to_(device_id)

    #  Test that scaling works as expected.
    t = torch.tensor([1.0], device=device_id)
    assert scaler.scale([torch.tensor([1.0], device=device_id)]) == [
        torch.tensor([2.0**16], device=device_id)
    ]
    assert scaler.scale(torch.tensor([1.0], device=device_id)) == torch.tensor(
        [2.0**16], device=device_id
    )
    with pytest.raises(ValueError):
        scaler.scale("bogus")
    with pytest.raises(ValueError):
        scaler.scale(42)

    # Test infinity detection.
    g = [
        torch.tensor([2.0**16], device=device_id),
        torch.tensor([float("Inf")], device=device_id),
    ]

    # Check that infinity was found.
    assert scaler.unscale(g)

    # Check whether unscale was successful.
    assert g[0] == torch.tensor([1.0]).cuda()

    scaler.update()

    # Since infinity was found, the scale should be halved from 2**16
    # to 2**15 for the next step.
    assert scaler.scale([torch.tensor([1.0], device=device_id)]) == [
        torch.tensor([2.0**15], device=device_id)
    ]


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.skipif(
    has_torch_amp, reason="needs PyTorch without gradient scaling support"
)
def test_raises_on_old_pytorch():
    import torch

    scaler = PyTorchGradScaler(enabled=True)
    with pytest.raises(ValueError, match=r"not supported.*1.9.0"):
        scaler.scale([torch.tensor([1.0], device="cpu")])


@pytest.mark.skipif(not has_torch, reason="needs PyTorch")
@pytest.mark.skipif(
    not has_torch_amp, reason="needs PyTorch with gradient scaling support"
)
def test_raises_with_cpu_tensor():
    import torch

    scaler = PyTorchGradScaler(enabled=True)
    with pytest.raises(
        ValueError, match=r"Gradient scaling is only supported for CUDA tensors."
    ):
        scaler.scale([torch.tensor([1.0], device="cpu")])
