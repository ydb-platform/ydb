# mypy: ignore-errors
import copy
from typing import Any, cast

import srsly

from ..compat import mxnet as mx
from ..optimizers import Optimizer
from ..types import ArgsKwargs, FloatsXd
from ..util import (
    convert_recursive,
    get_array_module,
    make_tempfile,
    mxnet2xp,
    xp2mxnet,
)
from .shim import Shim


class MXNetShim(Shim):
    """Interface between a MXNet model and a Thinc Model. This container is
    *not* a Thinc Model subclass itself.
    """

    def __call__(self, inputs, is_train):
        if is_train:
            return self.begin_update(inputs)
        else:
            return self.predict(inputs), lambda a: ...

    def predict(self, inputs: ArgsKwargs) -> Any:
        """Pass inputs through to the underlying MXNet model, and return the
        output. No conversions are performed. The MXNet model is set into
        evaluation mode.
        """
        mx.autograd.set_training(train_mode=False)
        with mx.autograd.pause():
            outputs = self._model(*inputs.args, **inputs.kwargs)
        mx.autograd.set_training(train_mode=True)
        return outputs

    def begin_update(self, inputs: ArgsKwargs):
        """Pass the inputs through to the underlying MXNet model, keeping
        track of which items in the input are tensors requiring gradients.
        If the model returns a single value, it is converted into a one-element
        tuple. Return the outputs and a callback to backpropagate.
        """
        mx.autograd.set_training(train_mode=True)
        mx.autograd.set_recording(True)
        output = self._model(*inputs.args, **inputs.kwargs)

        def backprop(grads):
            mx.autograd.set_recording(False)
            mx.autograd.backward(*grads.args, **grads.kwargs)
            return convert_recursive(
                lambda x: hasattr(x, "grad"), lambda x: x.grad, inputs
            )

        return output, backprop

    def finish_update(self, optimizer: Optimizer):
        params = []
        grads = []
        shapes = []
        ctx = mx.current_context()
        for key, value in self._model.collect_params().items():
            grad = cast(FloatsXd, mxnet2xp(value.grad(ctx)))
            param = cast(FloatsXd, mxnet2xp(value.data(ctx)))
            params.append(param.ravel())
            grads.append(grad.ravel())
            shapes.append((param.size, param.shape))
        if not params:
            return
        xp = get_array_module(params[0])
        flat_params, flat_grads = optimizer(
            (self.id, "mxnet-shim"), xp.concatenate(params), xp.concatenate(grads)
        )
        start = 0
        for key, value in self._model.collect_params().items():
            size, shape = shapes.pop(0)
            param = flat_params[start : start + size].reshape(shape)
            value.set_data(xp2mxnet(param))
            value.zero_grad()
            start += size

    def copy(self, ctx: "mx.context.Context" = None):
        if ctx is None:
            ctx = mx.current_context()
        model_bytes = self.to_bytes()
        copied = copy.deepcopy(self)
        copied._model.initialize(ctx=ctx)
        copied.from_bytes(model_bytes)
        return copied

    def to_device(self, device_type: str, device_id: int):
        if device_type == "cpu":
            self._model = self.copy(mx.cpu())
        elif device_type == "gpu":
            self._model = self.copy(mx.gpu())
        else:
            msg = f"Unexpected device_type: {device_type}. Try 'cpu' or 'gpu'."
            raise ValueError(msg)

    def to_bytes(self):
        # MXNet doesn't implement save/load without a filename
        with make_tempfile("w+b") as temp:
            self._model.save_parameters(temp.name)
            temp.seek(0)
            weights_bytes = temp.read()
        msg = {"config": self.cfg, "state": weights_bytes}
        return srsly.msgpack_dumps(msg)

    def from_bytes(self, bytes_data):
        msg = srsly.msgpack_loads(bytes_data)
        self.cfg = msg["config"]
        self._load_params(msg["state"])
        return self

    def _load_params(self, params):
        # MXNet doesn't implement save/load without a filename :(
        with make_tempfile("w+b") as temp:
            temp.write(params)
            self._model.load_parameters(temp.name, ctx=mx.current_context())
