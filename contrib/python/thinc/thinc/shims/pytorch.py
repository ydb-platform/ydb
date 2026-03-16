import contextlib
import itertools
from io import BytesIO
from typing import Any, Callable, Dict, Optional, cast

import srsly

from ..backends import CupyOps, context_pools, get_current_ops, set_gpu_allocator
from ..compat import torch
from ..optimizers import Optimizer
from ..types import ArgsKwargs, FloatsXd
from ..util import (
    convert_recursive,
    get_torch_default_device,
    iterate_recursive,
    torch2xp,
    xp2torch,
)
from .pytorch_grad_scaler import PyTorchGradScaler
from .shim import Shim


class PyTorchShim(Shim):
    """Interface between a PyTorch model and a Thinc Model. This container is
    *not* a Thinc Model subclass itself.

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
    serialize_model:
        Callback that receives the wrapped PyTorch model as its argument and
        returns a "bytes" representation of the same. The representation should
        contain all the necessary information to fully deserialize the model.
    deserialize_model:
        Callback that receives the default PyTorch model (passed to the constructor), the
        serialized "bytes" representation and a PyTorch device. It should return a
        fully deserialized model on the target device as its result.
    """

    def __init__(
        self,
        model: Any,
        config=None,
        optimizer: Any = None,
        mixed_precision: bool = False,
        grad_scaler: Optional[PyTorchGradScaler] = None,
        device: Optional["torch.device"] = None,
        serialize_model: Optional[Callable[[Any], bytes]] = None,
        deserialize_model: Optional[Callable[[Any, bytes, "torch.device"], Any]] = None,
    ):
        super().__init__(model, config, optimizer)

        if device is None:
            device = get_torch_default_device()
        if model is not None:
            model.to(device)

        if grad_scaler is None:
            grad_scaler = PyTorchGradScaler(mixed_precision)

        grad_scaler.to_(device)

        self._grad_scaler = grad_scaler
        self._mixed_precision = mixed_precision

        self._serialize_model = (
            serialize_model
            if serialize_model is not None
            else default_serialize_torch_model
        )
        self._deserialize_model = (
            deserialize_model
            if deserialize_model is not None
            else default_deserialize_torch_model
        )

        if CupyOps.xp is not None and isinstance(get_current_ops(), CupyOps):
            pools = context_pools.get()
            if "pytorch" not in pools:
                from cupy import get_default_memory_pool

                set_gpu_allocator("pytorch")
                get_default_memory_pool().free_all_blocks()

    def __call__(self, inputs, is_train):
        if is_train:
            return self.begin_update(inputs)
        else:
            return self.predict(inputs), lambda a: ...

    @property
    def device(self):
        p = next(self._model.parameters(), None)
        if p is None:
            return get_torch_default_device()
        else:
            return p.device

    def predict(self, inputs: ArgsKwargs) -> Any:
        """Pass inputs through to the underlying PyTorch model, and return the
        output. No conversions are performed. The PyTorch model is set into
        evaluation mode.
        """
        self._model.eval()
        with torch.no_grad():
            # NB: Previously this was torch.cuda.amp.autocast, passing a boolean
            # for mixed_precision. That doesn't seem to match the docs, and now
            # it raises an error when moving from the deprecated function. So
            # I've removed the argument but I'm not certain it's correct.
            with torch.autocast(device_type="cuda", enabled=self._mixed_precision):
                outputs = self._model(*inputs.args, **inputs.kwargs)
        self._model.train()
        return outputs

    def begin_update(self, inputs: ArgsKwargs):
        """Pass the inputs through to the underlying PyTorch model, keeping
        track of which items in the input are tensors requiring gradients.
        If the model returns a single value, it is converted into a one-element tuple.
        Return the outputs and a callback to backpropagate.
        """
        self._model.train()

        # Note: mixed-precision autocast must not be applied to backprop.
        # NB: Previously this was torch.cuda.amp.autocast, passing a boolean
        # for mixed_precision. That doesn't seem to match the docs, and now
        # it raises an error when moving from the deprecated function. So
        # I've removed the argument but I'm not certain it's correct.
        with torch.autocast("cuda", enabled=self._mixed_precision):
            output = self._model(*inputs.args, **inputs.kwargs)

        def backprop(grads):
            # Normally, gradient scaling is applied to the loss of a model. However,
            # since regular thinc layers do not use mixed-precision, we perform scaling
            # locally in this shim. Scaling the loss by a factor, scales the gradients
            # by the same factor (see the chain rule). Therefore, we scale the gradients
            # backprop'ed through the succeeding layer to get the same effect as loss
            # scaling.
            grads.kwargs["grad_tensors"] = self._grad_scaler.scale(
                grads.kwargs["grad_tensors"], inplace=True
            )

            torch.autograd.backward(*grads.args, **grads.kwargs)

            # Unscale weights and check for overflows during backprop.
            grad_tensors = []
            for torch_data in itertools.chain(
                self._model.parameters(),
                iterate_recursive(lambda x: hasattr(x, "grad"), inputs),
            ):
                if torch_data.grad is not None:
                    grad_tensors.append(torch_data.grad)
            found_inf = self._grad_scaler.unscale(grad_tensors)

            # If there was an over/underflow, return zeroed-out gradients.
            if found_inf:
                grad_get = lambda x: x.grad.zero_() if x.grad is not None else x.grad
            else:
                grad_get = lambda x: x.grad

            return convert_recursive(lambda x: hasattr(x, "grad"), grad_get, inputs)

        return output, backprop

    def finish_update(self, optimizer: Optimizer):
        for name, torch_data in self._model.named_parameters():
            if torch_data.grad is not None:
                if (
                    not self._grad_scaler.found_inf
                ):  # Skip weight update if any gradient overflowed.
                    param, grad = optimizer(
                        (self.id, name),
                        cast(FloatsXd, torch2xp(torch_data.data)),
                        cast(FloatsXd, torch2xp(torch_data.grad)),
                    )
                    torch_data.data = xp2torch(
                        param, requires_grad=True, device=torch_data.device
                    )
                torch_data.grad.zero_()

        self._grad_scaler.update()

    @contextlib.contextmanager
    def use_params(self, params):
        key_prefix = f"pytorch_{self.id}_"
        state_dict = {}
        for k, v in params.items():
            if hasattr(k, "startswith") and k.startswith(key_prefix):
                state_dict[k.replace(key_prefix, "")] = xp2torch(v, device=self.device)
        if state_dict:
            backup = {k: v.clone() for k, v in self._model.state_dict().items()}
            self._model.load_state_dict(state_dict)
            yield
            self._model.load_state_dict(backup)
        else:
            yield

    def to_device(self, device_type: str, device_id: int):  # pragma: no cover
        if device_type == "cpu":
            self._model.cpu()
        elif device_type == "gpu":
            self._model.cuda(device_id)
        else:
            msg = f"Invalid device_type: {device_type}. Try 'cpu' or 'gpu'"
            raise ValueError(msg)

    def to_bytes(self):
        model_bytes = self._serialize_model(self._model)
        msg = {"config": self.cfg, "state": model_bytes}
        return srsly.msgpack_dumps(msg)

    def from_bytes(self, bytes_data):
        device = get_torch_default_device()
        msg = srsly.msgpack_loads(bytes_data)
        self.cfg = msg["config"]
        self._model = self._deserialize_model(self._model, msg["state"], device)
        self._grad_scaler.to_(device)
        return self


def default_serialize_torch_model(model: Any) -> bytes:
    """Serializes the parameters of the wrapped PyTorch model to bytes.

    model:
        Wrapped PyTorch model.

    Returns:
        A `bytes` object that encapsulates the serialized model parameters.
    """
    filelike = BytesIO()
    torch.save(model.state_dict(), filelike)
    filelike.seek(0)
    return filelike.getvalue()


def default_deserialize_torch_model(
    model: Any, state_bytes: bytes, device: "torch.device"
) -> Any:
    """Deserializes the parameters of the wrapped PyTorch model and
    moves it to the specified device.

    model:
        Wrapped PyTorch model.
    state_bytes:
        Serialized parameters as a byte stream.
    device:
        PyTorch device to which the model is bound.

    Returns:
        The deserialized model.
    """
    filelike = BytesIO(state_bytes)
    filelike.seek(0)
    model.load_state_dict(torch.load(filelike, map_location=device))
    model.to(device)
    return model
