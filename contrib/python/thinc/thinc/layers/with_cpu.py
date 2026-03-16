from typing import Any, Callable, Tuple

import numpy

from thinc.backends import Ops

from ..config import registry
from ..model import Model


@registry.layers("with_cpu.v1")
def with_cpu(layer: Model, ops: Ops) -> Model:
    layer.to_cpu()
    return Model(
        f"with_cpu({layer.name})",
        forward,
        layers=[layer],
        ops=ops,
        init=init,
        dims={name: layer.maybe_get_dim(name) for name in layer.dim_names},
    )


def forward(model: Model, X: Any, is_train: bool) -> Tuple[Any, Callable]:
    cpu_outputs, backprop = model.layers[0].begin_update(_to_cpu(X))
    gpu_outputs = _to_device(model.ops, cpu_outputs)

    def with_cpu_backprop(d_outputs):
        cpu_d_outputs = _to_cpu(d_outputs)
        return backprop(cpu_d_outputs)

    return gpu_outputs, with_cpu_backprop


def init(model: Model, X: Any, Y: Any) -> None:
    model.layers[0].initialize(X, Y)


def _to_cpu(X):
    if isinstance(X, numpy.ndarray):
        return X
    elif isinstance(X, tuple):
        return tuple([_to_cpu(x) for x in X])
    elif isinstance(X, list):
        return [_to_cpu(x) for x in X]
    elif hasattr(X, "get"):
        return X.get()
    else:
        return X


def _to_device(ops, X):
    if isinstance(X, tuple):
        return tuple([_to_device(ops, x) for x in X])
    elif isinstance(X, list):
        return [_to_device(ops, x) for x in X]
    else:
        return ops.asarray(X)
