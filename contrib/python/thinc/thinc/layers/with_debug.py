from typing import Any, Callable, Optional, Tuple, TypeVar

from ..model import Model

_ModelT = TypeVar("_ModelT", bound=Model)

do_nothing = lambda *args, **kwargs: None


def with_debug(
    layer: _ModelT,
    name: Optional[str] = None,
    *,
    on_init: Callable[[Model, Any, Any], None] = do_nothing,
    on_forward: Callable[[Model, Any, bool], None] = do_nothing,
    on_backprop: Callable[[Any], None] = do_nothing,
) -> _ModelT:
    """Debugging layer that wraps any layer and allows executing callbacks
    during the forward pass, backward pass and initialization. The callbacks
    will receive the same arguments as the functions they're called in.
    """
    name = layer.name if name is None else name

    orig_forward = layer._func
    orig_init = layer.init

    def forward(model: Model, X: Any, is_train: bool) -> Tuple[Any, Callable]:
        on_forward(model, X, is_train)
        layer_Y, layer_callback = orig_forward(layer, X, is_train=is_train)

        def backprop(dY: Any) -> Any:
            on_backprop(dY)
            return layer_callback(dY)

        return layer_Y, backprop

    def init(model: Model, X: Any, Y: Any) -> None:
        on_init(model, X, Y)
        if orig_init is not None:
            orig_init(layer, X, Y)

    layer.replace_callbacks(forward, init=init)

    return layer
