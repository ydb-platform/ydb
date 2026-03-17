import asyncio
import base64
import functools
import struct
import sys
import time
import traceback
import weakref
from asyncio.coroutines import _format_coroutine  # type: ignore
from typing import Any, Callable, Coroutine, List, Optional, TypeVar

import janus
from typing_extensions import ParamSpec

from .types import CancellationChain, TerminatedTaskInfo
from .utils import _extract_stack_from_frame

__all__ = (
    "TracedTask",
    "preserve_termination_log",
    "persistent_coro",
)

persistent_coro: "weakref.WeakSet[Coroutine]" = weakref.WeakSet()

T = TypeVar("T")
P = ParamSpec("P")


class TracedTask(asyncio.Task):
    _orig_coro: Coroutine[Any, Any, Any]
    _termination_stack: Optional[List[traceback.FrameSummary]]

    def __init__(
        self,
        *args,
        termination_info_queue: janus._SyncQueueProxy[TerminatedTaskInfo],
        cancellation_chain_queue: janus._SyncQueueProxy[CancellationChain],
        persistent: bool = False,
        **kwargs,
    ) -> None:
        if sys.version_info < (3, 11):
            kwargs.pop("context")
        super().__init__(*args, **kwargs)
        self._termination_info_queue = termination_info_queue
        self._cancellation_chain_queue = cancellation_chain_queue
        self._started_at = time.perf_counter()
        self._termination_stack = None
        self.add_done_callback(self._trace_termination)
        self._persistent = persistent

    def get_trace_id(self) -> str:
        h = hash((
            id(self),
            self.get_name(),
        ))
        b = struct.pack("P", h)
        return base64.b32encode(b).rstrip(b"=").decode()

    def _trace_termination(self, _: "asyncio.Task[Any]") -> None:
        self_id = self.get_trace_id()
        exc_repr = (
            repr(self.exception())
            if not self.cancelled() and self.exception()
            else None
        )
        task_info = TerminatedTaskInfo(
            self_id,
            name=self.get_name(),
            coro=_format_coroutine(self._orig_coro).partition(" ")[0],
            started_at=self._started_at,
            terminated_at=time.perf_counter(),
            cancelled=self.cancelled(),
            termination_stack=self._termination_stack,
            canceller_stack=None,
            exc_repr=exc_repr,
            persistent=self._persistent,
        )
        self._termination_info_queue.put_nowait(task_info)

    def cancel(self, msg: Optional[str] = None) -> bool:
        try:
            canceller_task = asyncio.current_task()
        except RuntimeError:
            canceller_task = None
        if canceller_task is not None and isinstance(canceller_task, TracedTask):
            canceller_stack = _extract_stack_from_frame(sys._getframe())[:-1]
            cancellation_chain = CancellationChain(
                self.get_trace_id(),
                canceller_task.get_trace_id(),
                canceller_stack,
            )
            self._cancellation_chain_queue.put_nowait(cancellation_chain)
        return super().cancel(msg)


def preserve_termination_log(
    corofunc: Callable[P, Coroutine[Any, None, T]],
) -> Callable[P, Coroutine[Any, None, T]]:
    """
    Guard the given coroutine function from being stripped out due to the max history
    limit when created as TracedTask.
    """

    @functools.wraps(corofunc)
    def inner(*args: P.args, **kwargs: P.kwargs) -> Coroutine[Any, None, T]:
        coro = corofunc(*args, **kwargs)
        persistent_coro.add(coro)
        return coro

    return inner
