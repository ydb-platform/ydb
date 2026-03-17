import asyncio
import sys
import traceback
from collections.abc import Coroutine
from typing import TYPE_CHECKING, Generic, Optional, TypeVar

if sys.version_info >= (3, 11):
    from asyncio import timeout as asyncio_timeout
else:
    from async_timeout import timeout as asyncio_timeout

if TYPE_CHECKING:
    from ._scheduler import Scheduler
else:
    Scheduler = None

_T = TypeVar("_T", covariant=True)


class Job(Generic[_T]):
    def __init__(
        self,
        coro: Coroutine[object, object, _T],
        scheduler: Scheduler,
        name: Optional[str] = None,
    ):
        self._coro = coro
        self._scheduler: Optional[Scheduler] = scheduler
        self._name = name
        loop = asyncio.get_running_loop()
        self._started = loop.create_future()

        self._closed = False
        self._explicit = False
        self._task: Optional[asyncio.Task[_T]] = None

        tb = traceback.extract_stack(sys._getframe(2)) if loop.get_debug() else None
        self._source_traceback = tb

    def __repr__(self) -> str:
        info = []
        if self._closed:
            info.append("closed")
        elif self._task is None:
            info.append("pending")
        state = " ".join(info)
        if state:
            state += " "
        return f"<Job {state}coro=<{self._coro}>>"

    @property
    def active(self) -> bool:
        return not self.closed and not self.pending

    @property
    def pending(self) -> bool:
        return self._task is None and not self.closed

    @property
    def closed(self) -> bool:
        return self._closed

    def get_name(self) -> Optional[str]:
        """Get the task name.

        See https://docs.python.org/3/library/asyncio-task.html#asyncio.Task.get_name.
        Returns None if no name was set on the Job object and job has not yet started.
        """
        return self._task.get_name() if self._task else self._name

    def set_name(self, name: str) -> None:
        self._name = name
        if self._task is not None:
            self._task.set_name(name)

    async def _do_wait(self, timeout: Optional[float]) -> _T:
        async with asyncio_timeout(timeout):
            # TODO: add a test for waiting for a pending coro
            await self._started
            assert self._task is not None  # Task should have been created before this.
            return await self._task

    async def _wait(self, *, timeout: Optional[float] = None) -> _T:
        assert self._scheduler is not None  # Only removed when not _closed.
        scheduler = self._scheduler
        try:
            return await asyncio.shield(self._do_wait(timeout))
        except asyncio.CancelledError:
            # Don't stop inner coroutine on explicit cancel
            raise
        except Exception:
            await self._close(scheduler.close_timeout)
            raise

    async def wait(self, *, timeout: Optional[float] = None) -> _T:
        if self._closed:
            assert self._task is not None  # Task must have been created if closed.
            return await self._task
        self._explicit = True
        return await self._wait(timeout=timeout)

    async def close(self, *, timeout: Optional[float] = None) -> None:
        if self._closed:
            return
        self._explicit = True
        if timeout is None:
            assert self._scheduler is not None  # Only removed when not _closed.
            timeout = self._scheduler.close_timeout
        await self._close(timeout)

    async def _close(self, timeout: Optional[float]) -> None:
        self._closed = True
        if self._task is None:
            # the task is closed immediately without actual execution
            # it prevents a warning like
            # RuntimeWarning: coroutine 'coro' was never awaited
            self._start()
            assert self._task is not None
        self._task.cancel()
        # self._scheduler is None after _done_callback()
        scheduler = self._scheduler
        try:
            async with asyncio_timeout(timeout):
                await self._task
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError as exc:
            if self._explicit:
                raise
            context = {
                "message": "Job closing timed out",
                "job": self,
                "exception": exc,
            }
            if self._source_traceback is not None:
                context["source_traceback"] = self._source_traceback
            # scheduler is only None if job was already finished, in which case
            # there's no timeout. self._scheduler will now be None though.
            assert scheduler is not None
            scheduler.call_exception_handler(context)
        except Exception:
            if self._explicit:
                raise

    def _start(self) -> None:
        assert self._task is None
        self._task = asyncio.create_task(self._coro, name=self._name)
        self._task.add_done_callback(self._done_callback)
        self._started.set_result(None)

    def _done_callback(self, task: "asyncio.Task[_T]") -> None:
        assert self._scheduler is not None
        scheduler = self._scheduler
        scheduler._done(self)
        try:
            exc = task.exception()
        except asyncio.CancelledError:
            pass
        else:
            if exc is not None and not self._explicit:
                self._report_exception(exc)
                scheduler._failed_tasks.put_nowait(task)
        self._scheduler = None  # drop backref
        self._closed = True

    def _report_exception(self, exc: BaseException) -> None:
        assert self._scheduler is not None
        context = {"message": "Job processing failed", "job": self, "exception": exc}
        if self._source_traceback is not None:
            context["source_traceback"] = self._source_traceback
        self._scheduler.call_exception_handler(context)
