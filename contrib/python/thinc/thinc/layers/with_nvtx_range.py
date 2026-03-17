from typing import Any, Callable, Optional, Tuple, TypeVar

from ..model import Model
from ..util import use_nvtx_range

_ModelT = TypeVar("_ModelT", bound=Model)


def with_nvtx_range(
    layer: _ModelT,
    name: Optional[str] = None,
    *,
    forward_color: int = -1,
    backprop_color: int = -1,
) -> _ModelT:
    """Wraps any layer and marks the forward and backprop phases as
    NVTX ranges for CUDA profiling.

    By default, the name of the layer is used as the name of the range,
    followed by the name of the pass.
    """
    name = layer.name if name is None else name

    orig_forward = layer._func
    orig_init = layer.init

    def forward(model: Model, X: Any, is_train: bool) -> Tuple[Any, Callable]:
        with use_nvtx_range(f"{name} forward", forward_color):
            layer_Y, layer_callback = orig_forward(model, X, is_train=is_train)

        def backprop(dY: Any) -> Any:
            with use_nvtx_range(f"{name} backprop", backprop_color):
                return layer_callback(dY)

        return layer_Y, backprop

    def init(_model: Model, X: Any, Y: Any) -> Model:
        if orig_init is not None:
            return orig_init(layer, X, Y)
        else:
            return layer

    layer.replace_callbacks(forward, init=init)

    return layer
