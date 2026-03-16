from typing import Any, Callable, Optional, Tuple, TypeVar

from ..compat import has_os_signpost, os_signpost
from ..model import Model

_ModelT = TypeVar("_ModelT", bound=Model)


def with_signpost_interval(
    layer: _ModelT,
    signposter: "os_signpost.Signposter",
    name: Optional[str] = None,
) -> _ModelT:
    """Wraps any layer and marks the init, forward and backprop phases using
    signpost intervals for macOS Instruments profiling

    By default, the name of the layer is used as the name of the range,
    followed by the name of the pass.
    """
    if not has_os_signpost:
        raise ValueError(
            "with_signpost_interval layer requires the 'os_signpost' package"
        )

    name = layer.name if name is None else name

    orig_forward = layer._func
    orig_init = layer.init

    def forward(model: Model, X: Any, is_train: bool) -> Tuple[Any, Callable]:
        with signposter.use_interval(f"{name} forward"):
            layer_Y, layer_callback = orig_forward(model, X, is_train=is_train)

        def backprop(dY: Any) -> Any:
            with signposter.use_interval(f"{name} backprop"):
                return layer_callback(dY)

        return layer_Y, backprop

    def init(_model: Model, X: Any, Y: Any) -> Model:
        if orig_init is not None:
            with signposter.use_interval(f"{name} init"):
                return orig_init(layer, X, Y)
        else:
            return layer

    layer.replace_callbacks(forward, init=init)

    return layer
