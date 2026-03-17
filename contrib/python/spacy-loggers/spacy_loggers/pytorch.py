"""
A logger that queries PyTorch metrics and passes that information to downstream loggers.
"""
from typing import Dict, Any, Optional, Tuple, IO
import re
import sys

from spacy import Language
from .util import LoggerT


def pytorch_logger_v1(
    prefix: str = "pytorch",
    device: int = 0,
    cuda_mem_pool: str = "all",
    cuda_mem_metric: str = "all",
) -> LoggerT:
    try:
        import torch
    except ImportError:
        raise ImportError(
            "The 'torch' library could not be found - did you install it? "
            "Alternatively, specify the 'ConsoleLogger' in the "
            "'training.logger' config section, instead of the 'PyTorchLogger'."
        )

    def setup_logger(nlp: Language, stdout: IO = sys.stdout, stderr: IO = sys.stderr):
        expected_cuda_mem_pool = ("all", "large_pool", "small_pool")
        expected_cuda_mem_metric = ("all", "current", "peak", "allocated", "free")

        if cuda_mem_pool not in expected_cuda_mem_pool:
            raise ValueError(
                f"Got CUDA memory pool '{cuda_mem_pool}', but expected one of: '{expected_cuda_mem_pool}'"
            )
        elif cuda_mem_metric not in expected_cuda_mem_metric:
            raise ValueError(
                f"Got CUDA memory metric '{cuda_mem_metric}', but expected one of: '{expected_cuda_mem_metric}'"
            )

        def normalize_mem_value_to_mb(name: str, value: int) -> Tuple[str, float]:
            if "_bytes" in name:
                return re.sub("_bytes", "_megabytes", name), value / (1024.0**2)
            else:
                return name, value

        def log_step(info: Optional[Dict[str, Any]]):
            if info is None:
                return

            cuda_mem_stats = torch.cuda.memory_stats(device)
            for stat, val in cuda_mem_stats.items():
                splits = stat.split(".")
                if len(splits) == 3:
                    name, pool, metric = splits
                    name, val = normalize_mem_value_to_mb(name, val)
                    if pool != cuda_mem_pool:
                        continue
                    elif cuda_mem_metric != "all" and metric != cuda_mem_metric:
                        continue
                    info[f"{prefix}.{name}.{pool}.{metric}"] = val
                elif len(splits) == 2:
                    name, metric = splits
                    name, val = normalize_mem_value_to_mb(name, val)
                    if cuda_mem_metric != "all" and metric != cuda_mem_metric:
                        continue
                    info[f"{prefix}.{name}.{metric}"] = val
                else:
                    # Either global statistic or something that we haven't accounted for,
                    # e.g: a newly added statistic. So, we'll just include it to be safe.
                    info[f"{prefix}.{stat}"] = val

        def finalize():
            pass

        return log_step, finalize

    return setup_logger
