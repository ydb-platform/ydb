from typing import Any, Callable, Optional

from ..compat import torch
from ..model import Model
from ..shims import PyTorchGradScaler, PyTorchShim, TorchScriptShim
from .pytorchwrapper import (
    convert_pytorch_default_inputs,
    convert_pytorch_default_outputs,
    forward,
)


def TorchScriptWrapper_v1(
    torchscript_model: Optional["torch.jit.ScriptModule"] = None,
    convert_inputs: Optional[Callable] = None,
    convert_outputs: Optional[Callable] = None,
    mixed_precision: bool = False,
    grad_scaler: Optional[PyTorchGradScaler] = None,
    device: Optional["torch.device"] = None,
) -> Model[Any, Any]:
    """Wrap a TorchScript model, so that it has the same API as Thinc models.

    torchscript_model:
        The TorchScript module. A value of `None` is also possible to
        construct a shim to deserialize into.
    convert_inputs:
        Function that converts inputs and gradients that should be passed
        to the model to Torch tensors.
    convert_outputs:
        Function that converts model outputs and gradients from Torch tensors
        Thinc arrays.
    mixed_precision:
        Enable mixed-precision. This changes whitelisted ops to run
        in half precision for better performance and lower memory use.
    grad_scaler:
        The gradient scaler to use for mixed-precision training. If this
        argument is set to "None" and mixed precision is enabled, a gradient
        scaler with the default configuration is used.
    device:
        The PyTorch device to run the model on. When this argument is
        set to "None", the default device for the currently active Thinc
        ops is used.
    """

    if convert_inputs is None:
        convert_inputs = convert_pytorch_default_inputs
    if convert_outputs is None:
        convert_outputs = convert_pytorch_default_outputs

    return Model(
        "pytorch_script",
        forward,
        attrs={"convert_inputs": convert_inputs, "convert_outputs": convert_outputs},
        shims=[
            TorchScriptShim(
                model=torchscript_model,
                mixed_precision=mixed_precision,
                grad_scaler=grad_scaler,
                device=device,
            )
        ],
        dims={"nI": None, "nO": None},
    )


def pytorch_to_torchscript_wrapper(model: Model):
    """Convert a PyTorch wrapper to a TorchScript wrapper. The embedded PyTorch
    `Module` is converted to `ScriptModule`.
    """
    shim = model.shims[0]
    if not isinstance(shim, PyTorchShim):
        raise ValueError("Expected PyTorchShim when converting a PyTorch wrapper")

    convert_inputs = model.attrs["convert_inputs"]
    convert_outputs = model.attrs["convert_outputs"]

    pytorch_model = shim._model
    if not isinstance(pytorch_model, torch.nn.Module):
        raise ValueError("PyTorchShim does not wrap a PyTorch module")

    torchscript_model = torch.jit.script(pytorch_model)
    grad_scaler = shim._grad_scaler
    mixed_precision = shim._mixed_precision
    device = shim.device

    return TorchScriptWrapper_v1(
        torchscript_model,
        convert_inputs=convert_inputs,
        convert_outputs=convert_outputs,
        mixed_precision=mixed_precision,
        grad_scaler=grad_scaler,
        device=device,
    )
