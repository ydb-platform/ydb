"""
A logger that queries CuPy metrics and passes that information to downstream loggers.
"""
from typing import Dict, Any, Optional, IO
import sys

from spacy import Language
from thinc.backends import context_pools
from thinc.util import has_cupy_gpu
from .util import LoggerT


def cupy_logger_v1(
    prefix: str = "cupy",
) -> LoggerT:
    """Creates a logger that queries CuPy metrics and passes that information to downstream loggers.

    Args:
        prefix (str):
            All metric names are prefixed with this string using dot notation, e.g: `<prefix>.<metric>`.

    Returns:
        LoggerT: Logger instance.
    """
    try:
        import cupy
    except ImportError:
        raise ImportError(
            "The 'cupy' library could not be found - did you install it? "
            "Alternatively, specify the 'ConsoleLogger' in the "
            "'training.logger' config section, instead of the 'CuPyLogger'."
        )

    if not has_cupy_gpu:
        raise ValueError(
            "The 'cupy' library could be loaded but GPU support couldn't be initialized - "
            "do you have the necessary CUDA libraries installed?"
        )

    def setup_logger(nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr):
        def to_mib(bytes: int) -> float:
            return bytes / (1024.0**2)

        def log_step(info: Optional[Dict[str, Any]]):
            if info is None:
                return

            # We cannot rely on cupy.cuda.get_default_memory_pool() as thinc can
            # use its own allocator/pool for interop with PyTorch/TensorFlow.
            cupy_memory_pool = cupy.cuda.get_allocator().__self__
            thinc_pool = context_pools.get()

            if thinc_pool.get("pytorch") == cupy_memory_pool:
                pool_source = "pytorch"
            elif thinc_pool.get("tensorflow") == cupy_memory_pool:
                pool_source = "tensorflow"
            else:
                pool_source = "default"

            info[f"{prefix}.pool.source"] = pool_source
            info[f"{prefix}.pool.acquired_mib"] = to_mib(cupy_memory_pool.total_bytes())
            info[f"{prefix}.pool.used_mib"] = to_mib(cupy_memory_pool.used_bytes())
            info[f"{prefix}.pool.free_mib"] = to_mib(cupy_memory_pool.free_bytes())
            info[f"{prefix}.pool.num_free_blocks"] = cupy_memory_pool.n_free_blocks()

        def finalize():
            pass

        return log_step, finalize

    return setup_logger
