from typing import Optional

from .. import issues
from .._topic_common.common import _get_shared_event_loop, CallFromSyncToAsync
from ..aio.coordination.semaphore import CoordinationSemaphore as CoordinationSemaphoreAio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .session import CoordinationSession


class CoordinationSemaphore:
    def __init__(self, session: "CoordinationSession", name: str, limit: int = 1):
        self._session = session
        self._name = name
        self._limit = limit
        self._closed = False
        self._caller = CallFromSyncToAsync(_get_shared_event_loop())
        self._async_semaphore: CoordinationSemaphoreAio = self._session._async_session.semaphore(name, limit)

    def _check_closed(self):
        if self._closed:
            raise issues.Error(f"CoordinationSemaphore {self._name} already closed")

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.release()
        except Exception:
            pass

    def acquire(self, count: int = 1, timeout: Optional[float] = None):
        self._check_closed()
        return self._caller.safe_call_with_result(
            self._async_semaphore.acquire(count),
            timeout,
        )

    def release(self, timeout: Optional[float] = None):
        if self._closed:
            return
        return self._caller.safe_call_with_result(
            self._async_semaphore.release(),
            timeout,
        )

    def describe(self, timeout: Optional[float] = None):
        self._check_closed()
        return self._caller.safe_call_with_result(
            self._async_semaphore.describe(),
            timeout,
        )

    def update(self, new_data: bytes, timeout: Optional[float] = None):
        self._check_closed()
        return self._caller.safe_call_with_result(
            self._async_semaphore.update(new_data),
            timeout,
        )

    def close(self, timeout: Optional[float] = None):
        if self._closed:
            return
        try:
            self._caller.safe_call_with_result(
                self._async_semaphore.release(),
                timeout,
            )
        finally:
            self._closed = True
