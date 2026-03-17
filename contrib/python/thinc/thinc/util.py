import contextlib
import functools
import inspect
import os
import platform
import random
import tempfile
import threading
from contextvars import ContextVar
from dataclasses import dataclass
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import numpy
from packaging.version import Version
from pydantic import ConfigDict, ValidationError, create_model
from wasabi import table  # type: ignore

from .compat import (
    cupy,
    cupy_from_dlpack,
    has_cupy,
    has_cupy_gpu,
    has_gpu,
    has_mxnet,
    has_tensorflow,
    has_torch,
    has_torch_cuda_gpu,
    has_torch_mps,
)
from .compat import mxnet as mx
from .compat import tensorflow as tf
from .compat import torch

DATA_VALIDATION: ContextVar[bool] = ContextVar("DATA_VALIDATION", default=False)

from typing import TYPE_CHECKING

from . import types  # noqa: E402
from .types import ArgsKwargs, ArrayXd, FloatsXd, IntsXd, Padded, Ragged  # noqa: E402

if TYPE_CHECKING:
    from .api import Ops


def get_torch_default_device() -> "torch.device":
    if torch is None:
        raise ValueError("Cannot get default Torch device when Torch is not available.")

    from .backends import get_current_ops
    from .backends.cupy_ops import CupyOps
    from .backends.mps_ops import MPSOps

    ops = get_current_ops()
    if isinstance(ops, CupyOps):
        device_id = torch.cuda.current_device()
        return torch.device(f"cuda:{device_id}")
    elif isinstance(ops, MPSOps):
        return torch.device("mps")

    return torch.device("cpu")


def get_array_module(arr):  # pragma: no cover
    if is_numpy_array(arr):
        return numpy
    elif is_cupy_array(arr):
        return cupy
    else:
        raise ValueError(
            "Only numpy and cupy arrays are supported"
            f", but found {type(arr)} instead. If "
            "get_array_module module wasn't called "
            "directly, this might indicate a bug in Thinc."
        )


def gpu_is_available():
    return has_gpu


def fix_random_seed(seed: int = 0) -> None:  # pragma: no cover
    """Set the random seed across random, numpy.random and cupy.random."""
    random.seed(seed)
    numpy.random.seed(seed)
    if has_torch:
        torch.manual_seed(seed)
    if has_cupy_gpu:
        cupy.random.seed(seed)
        if has_torch and has_torch_cuda_gpu:
            torch.cuda.manual_seed_all(seed)
            torch.backends.cudnn.deterministic = True
            torch.backends.cudnn.benchmark = False


def is_xp_array(obj: Any) -> bool:
    """Check whether an object is a numpy or cupy array."""
    return is_numpy_array(obj) or is_cupy_array(obj)


def is_cupy_array(obj: Any) -> bool:  # pragma: no cover
    """Check whether an object is a cupy array."""
    if not has_cupy:
        return False
    elif isinstance(obj, cupy.ndarray):
        return True
    else:
        return False


def is_numpy_array(obj: Any) -> bool:
    """Check whether an object is a numpy array."""
    if isinstance(obj, numpy.ndarray):
        return True
    else:
        return False


def is_torch_array(obj: Any) -> bool:  # pragma: no cover
    if torch is None:
        return False
    elif isinstance(obj, torch.Tensor):
        return True
    else:
        return False


def is_torch_cuda_array(obj: Any) -> bool:  # pragma: no cover
    return is_torch_array(obj) and obj.is_cuda


def is_torch_gpu_array(obj: Any) -> bool:  # pragma: no cover
    return is_torch_cuda_array(obj) or is_torch_mps_array(obj)


def is_torch_mps_array(obj: Any) -> bool:  # pragma: no cover
    return is_torch_array(obj) and hasattr(obj, "is_mps") and obj.is_mps


def is_tensorflow_array(obj: Any) -> bool:  # pragma: no cover
    if not has_tensorflow:
        return False
    elif isinstance(obj, tf.Tensor):  # type: ignore
        return True
    else:
        return False


def is_tensorflow_gpu_array(obj: Any) -> bool:  # pragma: no cover
    return is_tensorflow_array(obj) and "GPU:" in obj.device


def is_mxnet_array(obj: Any) -> bool:  # pragma: no cover
    if not has_mxnet:
        return False
    elif isinstance(obj, mx.nd.NDArray):  # type: ignore
        return True
    else:
        return False


def is_mxnet_gpu_array(obj: Any) -> bool:  # pragma: no cover
    return is_mxnet_array(obj) and obj.context.device_type != "cpu"


def to_numpy(data):  # pragma: no cover
    if isinstance(data, numpy.ndarray):
        return data
    elif has_cupy and isinstance(data, cupy.ndarray):
        return data.get()
    else:
        return numpy.array(data)


def set_active_gpu(gpu_id: int) -> "cupy.cuda.Device":  # pragma: no cover
    """Set the current GPU device for cupy and torch (if available)."""
    if not has_cupy_gpu:
        raise ValueError("No CUDA GPU devices detected")

    device = cupy.cuda.device.Device(gpu_id)
    device.use()

    if has_torch_cuda_gpu:
        torch.cuda.set_device(gpu_id)

    return device


def require_cpu() -> bool:  # pragma: no cover
    """Use CPU through best available backend."""
    from .backends import get_ops, set_current_ops

    ops = get_ops("cpu")
    set_current_ops(ops)

    return True


def prefer_gpu(gpu_id: int = 0) -> bool:  # pragma: no cover
    """Use GPU if it's available. Returns True if so, False otherwise."""
    if has_gpu:
        require_gpu(gpu_id=gpu_id)
    return has_gpu


def require_gpu(gpu_id: int = 0) -> bool:  # pragma: no cover
    from .backends import CupyOps, MPSOps, set_current_ops

    if platform.system() == "Darwin" and not has_torch_mps:
        if has_torch:
            raise ValueError("Cannot use GPU, installed PyTorch does not support MPS")
        raise ValueError("Cannot use GPU, PyTorch is not installed")
    elif platform.system() != "Darwin" and not has_cupy:
        raise ValueError("Cannot use GPU, CuPy is not installed")
    elif not has_gpu:
        raise ValueError("No GPU devices detected")

    if has_cupy_gpu:
        set_current_ops(CupyOps())
        set_active_gpu(gpu_id)
    else:
        set_current_ops(MPSOps())

    return True


def copy_array(dst: ArrayXd, src: ArrayXd) -> None:  # pragma: no cover
    if isinstance(dst, numpy.ndarray) and isinstance(src, numpy.ndarray):
        dst[:] = src
    elif is_cupy_array(dst):
        src = cupy.array(src, copy=False)
        cupy.copyto(dst, src)
    else:
        numpy.copyto(dst, src)  # type: ignore


def to_categorical(
    Y: IntsXd,
    n_classes: Optional[int] = None,
    *,
    label_smoothing: float = 0.0,
) -> FloatsXd:
    if n_classes is None:
        n_classes = int(numpy.max(Y) + 1)  # type: ignore

    if label_smoothing < 0.0:
        raise ValueError(
            "Label-smoothing parameter has to be greater than or equal to 0"
        )

    if label_smoothing == 0.0:
        if n_classes == 0:
            raise ValueError("n_classes should be at least 1")
        nongold_prob = 0.0
    else:
        if not n_classes > 1:
            raise ValueError(
                "n_classes should be greater than 1 when label smoothing is enabled,"
                f"but {n_classes} was provided."
            )
        nongold_prob = label_smoothing / (n_classes - 1)

    max_smooth = (n_classes - 1) / n_classes
    if n_classes > 1 and label_smoothing >= max_smooth:
        raise ValueError(
            f"For {n_classes} classes "
            "label_smoothing parameter has to be less than "
            f"{max_smooth}, but found {label_smoothing}."
        )

    xp = get_array_module(Y)
    label_distr = xp.full((n_classes, n_classes), nongold_prob, dtype="float32")
    xp.fill_diagonal(label_distr, 1 - label_smoothing)
    return label_distr[Y]


def get_width(
    X: Union[ArrayXd, Ragged, Padded, Sequence[ArrayXd]], *, dim: int = -1
) -> int:
    """Infer the 'width' of a batch of data, which could be any of: Array,
    Ragged, Padded or Sequence of Arrays.
    """
    if isinstance(X, Ragged):
        return get_width(X.data, dim=dim)
    elif isinstance(X, Padded):
        return get_width(X.data, dim=dim)
    elif hasattr(X, "shape") and hasattr(X, "ndim"):
        X = cast(ArrayXd, X)
        if len(X.shape) == 0:
            return 0
        elif len(X.shape) == 1:
            return int(X.max()) + 1
        else:
            return X.shape[dim]
    elif isinstance(X, (list, tuple)):
        if len(X) == 0:
            return 0
        else:
            return get_width(X[0], dim=dim)
    else:
        err = "Cannot get width of object: has neither shape nor __getitem__"
        raise ValueError(err)


def assert_tensorflow_installed() -> None:  # pragma: no cover
    """Raise an ImportError if TensorFlow is not installed."""
    template = "TensorFlow support requires {pkg}: pip install thinc[tensorflow]\n\nEnable TensorFlow support with thinc.api.enable_tensorflow()"
    if not has_tensorflow:
        raise ImportError(template.format(pkg="tensorflow>=2.0.0,<2.6.0"))


def assert_mxnet_installed() -> None:  # pragma: no cover
    """Raise an ImportError if MXNet is not installed."""
    if not has_mxnet:
        raise ImportError(
            "MXNet support requires mxnet: pip install thinc[mxnet]\n\nEnable MXNet support with thinc.api.enable_mxnet()"
        )


def assert_pytorch_installed() -> None:  # pragma: no cover
    """Raise an ImportError if PyTorch is not installed."""
    if not has_torch:
        raise ImportError("PyTorch support requires torch: pip install thinc[torch]")


def convert_recursive(
    is_match: Callable[[Any], bool], convert_item: Callable[[Any], Any], obj: Any
) -> Any:
    """Either convert a single value if it matches a given function, or
    recursively walk over potentially nested lists, tuples and dicts applying
    the conversion, and returns the same type. Also supports the ArgsKwargs
    dataclass.
    """
    if is_match(obj):
        return convert_item(obj)
    elif isinstance(obj, ArgsKwargs):
        converted = convert_recursive(is_match, convert_item, list(obj.items()))
        return ArgsKwargs.from_items(converted)
    elif isinstance(obj, dict):
        converted = {}
        for key, value in obj.items():
            key = convert_recursive(is_match, convert_item, key)
            value = convert_recursive(is_match, convert_item, value)
            converted[key] = value
        return converted
    elif isinstance(obj, list):
        return [convert_recursive(is_match, convert_item, item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_recursive(is_match, convert_item, item) for item in obj)
    else:
        return obj


def iterate_recursive(is_match: Callable[[Any], bool], obj: Any) -> Any:
    """Either yield a single value if it matches a given function, or recursively
    walk over potentially nested lists, tuples and dicts yielding matching
    values. Also supports the ArgsKwargs dataclass.
    """
    if is_match(obj):
        yield obj
    elif isinstance(obj, ArgsKwargs):
        yield from iterate_recursive(is_match, list(obj.items()))
    elif isinstance(obj, dict):
        for key, value in obj.items():
            yield from iterate_recursive(is_match, key)
            yield from iterate_recursive(is_match, value)
    elif isinstance(obj, list) or isinstance(obj, tuple):
        for item in obj:
            yield from iterate_recursive(is_match, item)


def xp2torch(
    xp_tensor: ArrayXd,
    requires_grad: bool = False,
    device: Optional["torch.device"] = None,
) -> "torch.Tensor":  # pragma: no cover
    """Convert a numpy or cupy tensor to a PyTorch tensor."""
    assert_pytorch_installed()

    if device is None:
        device = get_torch_default_device()

    if hasattr(xp_tensor, "toDlpack"):
        dlpack_tensor = xp_tensor.toDlpack()  # type: ignore
        torch_tensor = torch.utils.dlpack.from_dlpack(dlpack_tensor)
    elif hasattr(xp_tensor, "__dlpack__"):
        torch_tensor = torch.utils.dlpack.from_dlpack(xp_tensor)
    else:
        torch_tensor = torch.from_numpy(xp_tensor)

    torch_tensor = torch_tensor.to(device)

    if requires_grad:
        torch_tensor.requires_grad_()

    return torch_tensor


def torch2xp(
    torch_tensor: "torch.Tensor", *, ops: Optional["Ops"] = None
) -> ArrayXd:  # pragma: no cover
    """Convert a torch tensor to a numpy or cupy tensor depending on the `ops` parameter.
    If `ops` is `None`, the type of the resultant tensor will be determined by the source tensor's device.
    """
    from .api import NumpyOps

    assert_pytorch_installed()
    if is_torch_cuda_array(torch_tensor):
        if isinstance(ops, NumpyOps):
            return torch_tensor.detach().cpu().numpy()
        else:
            return cupy_from_dlpack(torch.utils.dlpack.to_dlpack(torch_tensor))
    else:
        if isinstance(ops, NumpyOps) or ops is None:
            return torch_tensor.detach().cpu().numpy()
        else:
            return cupy.asarray(torch_tensor)


def xp2tensorflow(
    xp_tensor: ArrayXd, requires_grad: bool = False, as_variable: bool = False
) -> "tf.Tensor":  # type: ignore  # pragma: no cover
    """Convert a numpy or cupy tensor to a TensorFlow Tensor or Variable"""
    assert_tensorflow_installed()
    if hasattr(xp_tensor, "toDlpack"):
        dlpack_tensor = xp_tensor.toDlpack()  # type: ignore
        tf_tensor = tf.experimental.dlpack.from_dlpack(dlpack_tensor)  # type: ignore
    elif hasattr(xp_tensor, "__dlpack__"):
        dlpack_tensor = xp_tensor.__dlpack__()  # type: ignore
        tf_tensor = tf.experimental.dlpack.from_dlpack(dlpack_tensor)  # type: ignore
    else:
        tf_tensor = tf.convert_to_tensor(xp_tensor)  # type: ignore
    if as_variable:
        # tf.Variable() automatically puts in GPU if available.
        # So we need to control it using the context manager
        with tf.device(tf_tensor.device):  # type: ignore
            tf_tensor = tf.Variable(tf_tensor, trainable=requires_grad)  # type: ignore
    if requires_grad is False and as_variable is False:
        # tf.stop_gradient() automatically puts in GPU if available.
        # So we need to control it using the context manager
        with tf.device(tf_tensor.device):  # type: ignore
            tf_tensor = tf.stop_gradient(tf_tensor)  # type: ignore
    return tf_tensor


def tensorflow2xp(
    tf_tensor: "tf.Tensor", *, ops: Optional["Ops"] = None  # type: ignore
) -> ArrayXd:  # pragma: no cover
    """Convert a Tensorflow tensor to numpy or cupy tensor depending on the `ops` parameter.
    If `ops` is `None`, the type of the resultant tensor will be determined by the source tensor's device.
    """
    from .api import NumpyOps

    assert_tensorflow_installed()
    if is_tensorflow_gpu_array(tf_tensor):
        if isinstance(ops, NumpyOps):
            return tf_tensor.numpy()
        else:
            dlpack_tensor = tf.experimental.dlpack.to_dlpack(tf_tensor)  # type: ignore
            return cupy_from_dlpack(dlpack_tensor)
    else:
        if isinstance(ops, NumpyOps) or ops is None:
            return tf_tensor.numpy()
        else:
            return cupy.asarray(tf_tensor.numpy())


def xp2mxnet(
    xp_tensor: ArrayXd, requires_grad: bool = False
) -> "mx.nd.NDArray":  # type: ignore  # pragma: no cover
    """Convert a numpy or cupy tensor to a MXNet tensor."""
    assert_mxnet_installed()
    if hasattr(xp_tensor, "toDlpack"):
        dlpack_tensor = xp_tensor.toDlpack()  # type: ignore
        mx_tensor = mx.nd.from_dlpack(dlpack_tensor)  # type: ignore
    else:
        mx_tensor = mx.nd.from_numpy(xp_tensor)  # type: ignore
    if requires_grad:
        mx_tensor.attach_grad()
    return mx_tensor


def mxnet2xp(
    mx_tensor: "mx.nd.NDArray", *, ops: Optional["Ops"] = None  # type: ignore
) -> ArrayXd:  # pragma: no cover
    """Convert a MXNet tensor to a numpy or cupy tensor."""
    from .api import NumpyOps

    assert_mxnet_installed()
    if is_mxnet_gpu_array(mx_tensor):
        if isinstance(ops, NumpyOps):
            return mx_tensor.detach().asnumpy()
        else:
            return cupy_from_dlpack(mx_tensor.to_dlpack_for_write())
    else:
        if isinstance(ops, NumpyOps) or ops is None:
            return mx_tensor.detach().asnumpy()
        else:
            return cupy.asarray(mx_tensor.asnumpy())


# This is how functools.partials seems to do it, too, to retain the return type
PartialT = TypeVar("PartialT")


def partial(
    func: Callable[..., PartialT], *args: Any, **kwargs: Any
) -> Callable[..., PartialT]:
    """Wrapper around functools.partial that retains docstrings and can include
    other workarounds if needed.
    """
    partial_func = functools.partial(func, *args, **kwargs)
    partial_func.__doc__ = func.__doc__
    return partial_func


class DataValidationError(ValueError):
    def __init__(
        self,
        name: str,
        X: Any,
        Y: Any,
        errors: Union[Sequence[Mapping[str, Any]], List[Dict[str, Any]]] = [],
    ) -> None:
        """Custom error for validating inputs / outputs at runtime."""
        message = f"Data validation error in '{name}'"
        type_info = f"X: {type(X)} Y: {type(Y)}"
        data = []
        for error in errors:
            err_loc = " -> ".join([str(p) for p in error.get("loc", [])])
            data.append((err_loc, error.get("msg")))
        result = [message, type_info, table(data)]
        ValueError.__init__(self, "\n\n" + "\n".join(result))


def validate_fwd_input_output(
    name: str, func: Callable[[Any, Any, bool], Any], X: Any, Y: Any
) -> None:
    """Validate the input and output of a forward function against the type
    annotations, if available. Used in Model.initialize with the input and
    output samples as they pass through the network.
    """
    sig = inspect.signature(func)
    empty = inspect.Signature.empty
    params = list(sig.parameters.values())
    if len(params) != 3:
        bad_params = f"{len(params)} ({', '.join([p.name for p in params])})"
        err = f"Invalid forward function. Expected 3 arguments (model, X , is_train), got {bad_params}"
        raise DataValidationError(name, X, Y, [{"msg": err}])
    # Unfortunately I can't get this to work for Pydantic v2 yet. The Floats2d etc types fail to validate
    # against their duck-types numpy.ndarray etc. For now disable this validation while I check with
    # the pydantic folks how I should do this.
    # TODO: Uncomment when working
    # annot_x = params[1].annotation
    # annot_y = sig.return_annotation
    # sig_args: Dict[str, Any] = {"__config__": {"extra": "forbid", "arbitrary_types_allowed": True}}
    # args = {}
    # if X is not None and annot_x != empty:
    #     if isinstance(X, list) and len(X) > 5:
    #         X = X[:5]
    #     sig_args["X"] = (annot_x, ...)
    #     args["X"] = X
    # if Y is not None and annot_y != empty:
    #     if isinstance(Y, list) and len(Y) > 5:
    #         Y = Y[:5]
    #     sig_args["Y"] = (annot_y, ...)
    #     args["Y"] = (Y, lambda x: x)

    # ArgModel = create_model("ArgModel", X=sig_args["X"], Y=sig_args["Y"], __config__=ConfigDict(extra="forbid", arbitrary_types_allowed=True))
    # # Make sure the forward refs are resolved and the types used by them are
    # # available in the correct scope. See #494 for details.
    # ArgModel.model_rebuild()
    # try:
    #     ArgModel.model_validate(args)
    # except ValidationError as e:
    #     raise DataValidationError(name, X, Y, e.errors()) from None
    return None


@contextlib.contextmanager
def make_tempfile(mode="r"):
    f = tempfile.NamedTemporaryFile(mode=mode, delete=False)
    yield f
    f.close()
    os.remove(f.name)


@contextlib.contextmanager
def data_validation(validation):
    with threading.Lock():
        prev = DATA_VALIDATION.get()
        DATA_VALIDATION.set(validation)
        yield
        DATA_VALIDATION.set(prev)


@contextlib.contextmanager
def use_nvtx_range(message: str, id_color: int = -1):
    """Context manager to register the executed code as an NVTX range. The
    ranges can be used as markers in CUDA profiling."""
    if has_cupy:
        cupy.cuda.nvtx.RangePush(message, id_color)
        yield
        cupy.cuda.nvtx.RangePop()
    else:
        yield


@dataclass
class ArrayInfo:
    """Container for info for checking array compatibility."""

    shape: types.Shape
    dtype: types.DTypes

    @classmethod
    def from_array(cls, arr: ArrayXd):
        return cls(shape=arr.shape, dtype=arr.dtype)

    def check_consistency(self, arr: ArrayXd):
        if arr.shape != self.shape:
            raise ValueError(
                f"Shape mismatch in backprop. Y: {self.shape}, dY: {arr.shape}"
            )
        if arr.dtype != self.dtype:
            raise ValueError(
                f"Type mismatch in backprop. Y: {self.dtype}, dY: {arr.dtype}"
            )


# fmt: off
__all__ = [
    "get_array_module",
    "get_torch_default_device",
    "fix_random_seed",
    "is_cupy_array",
    "is_numpy_array",
    "set_active_gpu",
    "prefer_gpu",
    "require_gpu",
    "copy_array",
    "to_categorical",
    "get_width",
    "xp2torch",
    "torch2xp",
    "tensorflow2xp",
    "xp2tensorflow",
    "validate_fwd_input_output",
    "DataValidationError",
    "make_tempfile",
    "use_nvtx_range",
    "ArrayInfo",
    "has_cupy",
    "has_torch",
]
# fmt: on
