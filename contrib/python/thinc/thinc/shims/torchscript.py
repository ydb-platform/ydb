from io import BytesIO
from typing import Any, Optional

import srsly

from ..compat import torch
from ..util import get_torch_default_device
from .pytorch import PyTorchShim
from .pytorch_grad_scaler import PyTorchGradScaler


class TorchScriptShim(PyTorchShim):
    """A Thinc shim that wraps a TorchScript module.

    model:
        The TorchScript module. A value of `None` is also possible to
        construct a shim to deserialize into.
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

    def __init__(
        self,
        model: Optional["torch.jit.ScriptModule"],
        config=None,
        optimizer: Any = None,
        mixed_precision: bool = False,
        grad_scaler: Optional[PyTorchGradScaler] = None,
        device: Optional["torch.device"] = None,
    ):
        if model is not None and not isinstance(model, torch.jit.ScriptModule):
            raise ValueError(
                "PyTorchScriptShim must be initialized with ScriptModule or None (for deserialization)"
            )

        super().__init__(model, config, optimizer, mixed_precision, grad_scaler, device)

    def to_bytes(self):
        filelike = BytesIO()
        torch.jit.save(self._model, filelike)
        filelike.seek(0)
        model_bytes = filelike.getvalue()
        msg = {"config": self.cfg, "model": model_bytes}
        return srsly.msgpack_dumps(msg)

    def from_bytes(self, bytes_data):
        device = get_torch_default_device()
        msg = srsly.msgpack_loads(bytes_data)
        self.cfg = msg["config"]
        filelike = BytesIO(msg["model"])
        filelike.seek(0)
        # As of Torch 2.0.0, loading TorchScript models directly to
        # an MPS device is not supported.
        map_location = torch.device("cpu") if device.type == "mps" else device
        self._model = torch.jit.load(filelike, map_location=map_location)
        self._model.to(device)
        self._grad_scaler.to_(device)
        return self
