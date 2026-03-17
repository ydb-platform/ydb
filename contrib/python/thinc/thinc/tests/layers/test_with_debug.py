from mock import MagicMock

from thinc.api import Linear, with_debug


def test_with_debug():
    on_init = MagicMock()
    on_forward = MagicMock()
    on_backprop = MagicMock()
    model = with_debug(
        Linear(), on_init=on_init, on_forward=on_forward, on_backprop=on_backprop
    )
    on_init.assert_not_called()
    on_forward.assert_not_called()
    on_backprop.assert_not_called()
    X = model.ops.alloc2f(1, 1)
    Y = model.ops.alloc2f(1, 1)
    model.initialize(X=X, Y=Y)
    on_init.assert_called_once_with(model, X, Y)
    on_forward.assert_not_called()
    on_backprop.assert_not_called()
    Yh, backprop = model(X, is_train=True)
    on_forward.assert_called_once_with(model, X, True)
    on_backprop.assert_not_called()
    backprop(Y)
    on_backprop.assert_called_once_with(Y)
