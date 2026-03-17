from typing import Any, Callable, Optional, Tuple, Type

from ..config import registry
from ..model import Model
from ..shims import MXNetShim
from ..types import ArgsKwargs
from ..util import convert_recursive, is_mxnet_array, is_xp_array, mxnet2xp, xp2mxnet


@registry.layers("MXNetWrapper.v1")
def MXNetWrapper(
    mxnet_model,
    convert_inputs: Optional[Callable] = None,
    convert_outputs: Optional[Callable] = None,
    model_class: Type[Model] = Model,
    model_name: str = "mxnet",
) -> Model[Any, Any]:
    """Wrap a MXNet model, so that it has the same API as Thinc models.
    To optimize the model, you'll need to create a MXNet optimizer and call
    optimizer.step() after each batch.

    Your MXNet model's forward method can take arbitrary args and kwargs,
    but must return either a single tensor as output or a tuple. You may find the
    MXNet register_forward_hook helpful if you need to adapt the output.

    The convert functions are used to map inputs and outputs to and from your
    MXNet model. Each function should return the converted output, and a callback
    to use during the backward pass. So:

        Xmxnet, get_dX = convert_inputs(X)
        Ymxnet, mxnet_backprop = model.shims[0](Xmxnet, is_train)
        Y, get_dYmxnet = convert_outputs(Ymxnet)

    To allow maximum flexibility, the MXNetShim expects ArgsKwargs objects
    on the way into the forward and backward passes. The ArgsKwargs objects
    will be passed straight into the model in the forward pass, and straight
    into `mxnet.autograd.backward` during the backward pass.
    """
    if convert_inputs is None:
        convert_inputs = convert_mxnet_default_inputs
    if convert_outputs is None:
        convert_outputs = convert_mxnet_default_outputs
    return model_class(
        model_name,
        forward,
        attrs={"convert_inputs": convert_inputs, "convert_outputs": convert_outputs},
        shims=[MXNetShim(mxnet_model)],
    )


def forward(model: Model, X: Any, is_train: bool) -> Tuple[Any, Callable]:
    """Return the output of the wrapped MXNet model for the given input,
    along with a callback to handle the backward pass.
    """
    convert_inputs = model.attrs["convert_inputs"]
    convert_outputs = model.attrs["convert_outputs"]

    Xmxnet, get_dX = convert_inputs(model, X, is_train)
    Ymxnet, mxnet_backprop = model.shims[0](Xmxnet, is_train)
    Y, get_dYmxnet = convert_outputs(model, (X, Ymxnet), is_train)

    def backprop(dY: Any) -> Any:
        dYmxnet = get_dYmxnet(dY)
        dXmxnet = mxnet_backprop(dYmxnet)
        dX = get_dX(dXmxnet)
        return dX

    return Y, backprop


# Default conversion functions


def convert_mxnet_default_inputs(
    model: Model, X: Any, is_train: bool
) -> Tuple[ArgsKwargs, Callable[[ArgsKwargs], Any]]:
    xp2mxnet_ = lambda x: xp2mxnet(x, requires_grad=is_train)
    converted = convert_recursive(is_xp_array, xp2mxnet_, X)
    if isinstance(converted, ArgsKwargs):

        def reverse_conversion(dXmxnet):
            return convert_recursive(is_mxnet_array, mxnet2xp, dXmxnet)

        return converted, reverse_conversion
    elif isinstance(converted, dict):

        def reverse_conversion(dXmxnet):
            dX = convert_recursive(is_mxnet_array, mxnet2xp, dXmxnet)
            return dX.kwargs

        return ArgsKwargs(args=tuple(), kwargs=converted), reverse_conversion
    elif isinstance(converted, (tuple, list)):

        def reverse_conversion(dXmxnet):
            dX = convert_recursive(is_mxnet_array, mxnet2xp, dXmxnet)
            return dX.args

        return ArgsKwargs(args=tuple(converted), kwargs={}), reverse_conversion
    else:

        def reverse_conversion(dXmxnet):
            dX = convert_recursive(is_mxnet_array, mxnet2xp, dXmxnet)
            return dX.args[0]

        return ArgsKwargs(args=(converted,), kwargs={}), reverse_conversion


def convert_mxnet_default_outputs(model: Model, X_Ymxnet: Any, is_train: bool):
    X, Ymxnet = X_Ymxnet
    Y = convert_recursive(is_mxnet_array, mxnet2xp, Ymxnet)

    def reverse_conversion(dY: Any) -> ArgsKwargs:
        dYmxnet = convert_recursive(is_xp_array, xp2mxnet, dY)
        return ArgsKwargs(args=((Ymxnet,),), kwargs={"head_grads": dYmxnet})

    return Y, reverse_conversion
