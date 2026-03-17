from collections import deque
from collections.abc import Callable, Iterator
from typing import Any

__all__ = ["reprstream", "saferepr"]

def saferepr(
    o: Any, maxlen: int | None = ..., maxlevels: int = ..., seen: set[int] | None = ...
) -> str: ...
def reprstream(
    stack: deque[Any],
    seen: set[int] | None = ...,
    maxlevels: int = ...,
    level: int = ...,
    isinstance: Callable[[Any, Any], bool] = ...,
) -> Iterator[tuple[Any, Any]]: ...
