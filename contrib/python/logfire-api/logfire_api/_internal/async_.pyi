from .constants import ONE_SECOND_IN_NANOSECONDS as ONE_SECOND_IN_NANOSECONDS
from .main import Logfire as Logfire
from .stack_info import StackInfo as StackInfo, get_code_object_info as get_code_object_info, get_stack_info_from_frame as get_stack_info_from_frame
from .utils import safe_repr as safe_repr
from _typeshed import Incomplete
from contextlib import AbstractContextManager
from types import CoroutineType
from typing import Any

ASYNCIO_PATH: Incomplete

def log_slow_callbacks(logfire: Logfire, slow_duration: float) -> AbstractContextManager[None]:
    """Log a warning whenever a function running in the asyncio event loop blocks for too long.

    See Logfire.log_slow_async_callbacks.
    Inspired by https://gitlab.com/quantlane/libs/aiodebug.
    """

class _CallbackAttributes(StackInfo, total=False):
    name: str
    stack: list[StackInfo]

def stack_info_from_coroutine(coro: CoroutineType[Any, Any, Any]) -> StackInfo: ...
