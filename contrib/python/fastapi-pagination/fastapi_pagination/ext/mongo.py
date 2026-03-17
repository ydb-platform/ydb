__all__ = [
    "AggrPipelineTransformer",
]

from collections.abc import Callable
from typing import Any

AggrPipelineTransformer = Callable[[list[dict[str, Any]]], list[dict[str, Any]]]
