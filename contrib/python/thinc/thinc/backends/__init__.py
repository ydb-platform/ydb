import contextlib
import threading
from contextvars import ContextVar
from typing import Any, Callable, Dict, Optional, Type, cast

from .. import registry
from ..compat import cupy, has_cupy
from ..util import (
    assert_pytorch_installed,
    assert_tensorflow_installed,
    get_torch_default_device,
    is_cupy_array,
    require_cpu,
)
from ._cupy_allocators import cupy_pytorch_allocator, cupy_tensorflow_allocator
from ._param_server import ParamServer
from .cupy_ops import CupyOps
from .mps_ops import MPSOps
from .numpy_ops import NumpyOps
from .ops import Ops

context_ops: ContextVar[Optional[Ops]] = ContextVar("context_ops", default=None)
context_pools: ContextVar[dict] = ContextVar("context_pools", default={})

# Internal use of thread-local storage only for detecting cases where a Jupyter
# notebook might not have preserved contextvars across cells.
_GLOBAL_STATE = {"ops": None}


def set_gpu_allocator(allocator: str) -> None:  # pragma: no cover
    """Route GPU memory allocation via PyTorch or tensorflow.
    Raise an error if the given argument does not match either of the two.
    """
    if allocator == "pytorch":
        use_pytorch_for_gpu_memory()
    elif allocator == "tensorflow":
        use_tensorflow_for_gpu_memory()
    else:
        raise ValueError(
            f"Invalid 'gpu_allocator' argument: '{allocator}'. Available allocators are: 'pytorch', 'tensorflow'"
        )


def use_pytorch_for_gpu_memory() -> None:  # pragma: no cover
    """Route GPU memory allocation via PyTorch.

    This is recommended for using PyTorch and cupy together, as otherwise
    OOM errors can occur when there's available memory sitting in the other
    library's pool.

    We'd like to support routing Tensorflow memory allocation via PyTorch as well
    (or vice versa), but do not currently have an implementation for it.
    """
    assert_pytorch_installed()

    if get_torch_default_device().type != "cuda":
        return

    pools = context_pools.get()
    if "pytorch" not in pools:
        pools["pytorch"] = cupy.cuda.MemoryPool(allocator=cupy_pytorch_allocator)
    cupy.cuda.set_allocator(pools["pytorch"].malloc)


def use_tensorflow_for_gpu_memory() -> None:  # pragma: no cover
    """Route GPU memory allocation via TensorFlow.

    This is recommended for using TensorFlow and cupy together, as otherwise
    OOM errors can occur when there's available memory sitting in the other
    library's pool.

    We'd like to support routing PyTorch memory allocation via Tensorflow as
    well (or vice versa), but do not currently have an implementation for it.
    """
    assert_tensorflow_installed()
    pools = context_pools.get()
    if "tensorflow" not in pools:
        pools["tensorflow"] = cupy.cuda.MemoryPool(allocator=cupy_tensorflow_allocator)
    cupy.cuda.set_allocator(pools["tensorflow"].malloc)


def _import_extra_cpu_backends():
    try:
        from thinc_apple_ops import AppleOps
    except ImportError:
        pass
    try:
        from thinc_bigendian_ops import BigEndianOps
    except ImportError:
        pass


def get_ops(name: str, **kwargs) -> Ops:
    """Get a backend object.

    The special name "cpu" returns the best available CPU backend."""

    ops_by_name = {ops_cls.name: ops_cls for ops_cls in registry.ops.get_all().values()}  # type: ignore

    cls: Optional[Callable[..., Ops]] = None
    if name == "cpu":
        _import_extra_cpu_backends()
        cls = ops_by_name.get("numpy")
        cls = ops_by_name.get("apple", cls)
        cls = ops_by_name.get("bigendian", cls)
    else:
        cls = ops_by_name.get(name)

    if cls is None:
        raise ValueError(f"Invalid backend: {name}")

    return cls(**kwargs)


def get_array_ops(arr):
    """Return CupyOps for a cupy array, NumpyOps otherwise."""
    if is_cupy_array(arr):
        return CupyOps()
    else:
        return NumpyOps()


@contextlib.contextmanager
def use_ops(name: str, **kwargs):
    """Change the backend to execute on for the scope of the block."""
    current_ops = get_current_ops()
    set_current_ops(get_ops(name, **kwargs))
    try:
        yield
    finally:
        set_current_ops(current_ops)


def get_current_ops() -> Ops:
    """Get the current backend object."""
    if context_ops.get() is None:
        require_cpu()
    return cast(Ops, context_ops.get())


def set_current_ops(ops: Ops) -> None:
    """Change the current backend object."""
    context_ops.set(ops)
    _get_thread_state().ops = ops


def contextvars_eq_thread_ops() -> bool:
    current_ops = context_ops.get()
    thread_ops = _get_thread_state().ops
    if type(current_ops) == type(thread_ops):
        return True
    return False


def _get_thread_state():
    """Get a thread-specific state variable that inherits from a global
    state when it's created."""
    thread: threading.Thread = threading.current_thread()
    if not hasattr(thread, "__local"):
        thread.__local = _create_thread_local(_GLOBAL_STATE)
    return thread.__local


def _create_thread_local(
    attrs: Dict[str, Any], local_class: Type[threading.local] = threading.local
):
    obj = local_class()
    for name, value in attrs.items():
        setattr(obj, name, value)
    return obj


__all__ = [
    "set_current_ops",
    "get_current_ops",
    "use_ops",
    "ParamServer",
    "Ops",
    "CupyOps",
    "MPSOps",
    "NumpyOps",
    "has_cupy",
]
