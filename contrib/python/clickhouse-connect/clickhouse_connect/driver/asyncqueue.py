import asyncio
import threading
from collections import deque
from typing import Generic, TypeVar

from clickhouse_connect.driver.exceptions import ProgrammingError

__all__ = ["AsyncSyncQueue", "Empty", "Full", "EOF_SENTINEL"]

T = TypeVar("T")

EOF_SENTINEL = object()


class AsyncSyncQueue(Generic[T]):
    """High-performance bridge between AsyncIO and Threading."""

    def __init__(self, maxsize: int = 100):
        self._maxsize = maxsize
        self._queue: deque[T] = deque()
        self._shutdown = False
        self._loop: asyncio.AbstractEventLoop | None = None

        self._lock = threading.Lock()

        self._sync_not_empty = threading.Condition(self._lock)
        self._sync_not_full = threading.Condition(self._lock)

        self._async_getters: deque[asyncio.Future] = deque()
        self._async_putters: deque[asyncio.Future] = deque()

        self.sync_q = _SyncQueueInterface(self)
        self.async_q = _AsyncQueueInterface(self)

    def _bind_loop(self):
        """Lazy-bind to the running loop on first async access."""
        if self._loop is None:
            try:
                self._loop = asyncio.get_running_loop()
            except RuntimeError:
                pass

    def _check_deadlock(self):
        """Check if blocking would cause a deadlock on the event loop."""
        if self._loop is None:
            return

        try:
            current_loop = asyncio.get_running_loop()
            if current_loop is self._loop:
                raise ProgrammingError(
                    "Deadlock detected: Synchronous blocking operation called on event loop thread. "
                    "This usually happens when iterating a stream synchronously (e.g., 'for row in result') "
                    "instead of asynchronously ('async for row in result') inside an async function."
                )
        except RuntimeError:
            pass

    @staticmethod
    def _safe_set_result(fut: asyncio.Future):
        """Set result on a future only if it hasn't been cancelled or resolved.

        This runs on the event loop thread after being scheduled via
        call_soon_threadsafe. Between scheduling and execution the future
        may have been cancelled (e.g. by Task.cancel()), so the done()
        check must happen here, not at schedule time.
        """
        if not fut.done():
            fut.set_result(None)

    def _wakeup_async_waiter(self, waiter_queue: deque[asyncio.Future]):
        """Helper: Wake up the next async waiter in the queue safely."""
        while waiter_queue:
            fut = waiter_queue.popleft()
            if not fut.done():
                self._loop.call_soon_threadsafe(self._safe_set_result, fut)
                break

    def shutdown(self):
        """Terminates the queue. All readers will receive EOF_SENTINEL."""
        with self._lock:
            self._shutdown = True

            self._sync_not_empty.notify_all()
            self._sync_not_full.notify_all()

            if self._loop and not self._loop.is_closed():
                for fut in list(self._async_getters):
                    if not fut.done():
                        self._loop.call_soon_threadsafe(self._safe_set_result, fut)
                for fut in list(self._async_putters):
                    if not fut.done():
                        self._loop.call_soon_threadsafe(self._safe_set_result, fut)
                self._async_getters.clear()
                self._async_putters.clear()

    @property
    def qsize(self) -> int:
        with self._lock:
            return len(self._queue)


class _SyncQueueInterface(Generic[T]):
    def __init__(self, parent: AsyncSyncQueue[T]):
        self._p = parent

    def get(self, block: bool = True, timeout: float | None = None) -> T:
        with self._p._lock:
            while not self._p._queue and not self._p._shutdown:
                if not block:
                    raise Empty()

                self._p._check_deadlock()
                if not self._p._sync_not_empty.wait(timeout):
                    raise Empty()

            if not self._p._queue and self._p._shutdown:
                return EOF_SENTINEL

            item = self._p._queue.popleft()
            self._p._sync_not_full.notify()
            self._p._wakeup_async_waiter(self._p._async_putters)

            return item

    def put(self, item: T, block: bool = True, timeout: float | None = None) -> None:
        with self._p._lock:
            if self._p._shutdown:
                raise RuntimeError("Queue is shutdown")

            while self._p._maxsize > 0 and len(self._p._queue) >= self._p._maxsize:
                if not block:
                    raise Full()

                self._p._check_deadlock()
                if not self._p._sync_not_full.wait(timeout):
                    raise Full()
                if self._p._shutdown:
                    raise RuntimeError("Queue is shutdown")

            self._p._queue.append(item)

            self._p._sync_not_empty.notify()
            self._p._wakeup_async_waiter(self._p._async_getters)


class _AsyncQueueInterface(Generic[T]):
    def __init__(self, parent: AsyncSyncQueue[T]):
        self._p = parent

    async def get(self) -> T:
        self._p._bind_loop()
        while True:
            with self._p._lock:
                if self._p._queue:
                    item = self._p._queue.popleft()
                    self._p._sync_not_full.notify()
                    self._p._wakeup_async_waiter(self._p._async_putters)
                    return item

                if self._p._shutdown:
                    return EOF_SENTINEL

                fut = self._p._loop.create_future()
                self._p._async_getters.append(fut)

            try:
                await fut
            except asyncio.CancelledError:
                with self._p._lock:
                    if fut in self._p._async_getters:
                        self._p._async_getters.remove(fut)
                raise

    async def put(self, item: T) -> None:
        self._p._bind_loop()
        while True:
            with self._p._lock:
                if self._p._shutdown:
                    raise RuntimeError("Queue is shutdown")

                if self._p._maxsize <= 0 or len(self._p._queue) < self._p._maxsize:
                    self._p._queue.append(item)
                    self._p._sync_not_empty.notify()
                    self._p._wakeup_async_waiter(self._p._async_getters)
                    return

                fut = self._p._loop.create_future()
                self._p._async_putters.append(fut)

            try:
                await fut
            except asyncio.CancelledError:
                with self._p._lock:
                    if fut in self._p._async_putters:
                        self._p._async_putters.remove(fut)
                raise


class Empty(Exception):  # noqa: N818
    pass


class Full(Exception):  # noqa: N818
    pass
