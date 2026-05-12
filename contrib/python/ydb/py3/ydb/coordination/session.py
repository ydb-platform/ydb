from .._topic_common.common import _get_shared_event_loop, CallFromSyncToAsync
from ..aio.coordination.session import CoordinationSession as CoordinationSessionAio
from .semaphore import CoordinationSemaphore


class CoordinationSession:
    def __init__(self, client, path: str, timeout_sec: float = 5):
        self._client = client
        self._path = path
        self._timeout_sec = timeout_sec

        self._caller = CallFromSyncToAsync(_get_shared_event_loop())
        self._closed = False

        async def _make_session() -> CoordinationSessionAio:
            return CoordinationSessionAio(
                client._driver,
                path,
            )

        self._async_session: CoordinationSessionAio = self._caller.safe_call_with_result(
            _make_session(),
            self._timeout_sec,
        )

    def semaphore(self, name: str, limit: int = 1):
        return CoordinationSemaphore(self, name, limit)

    def close(self):
        if self._closed:
            return
        self._caller.safe_call_with_result(
            self._async_session.close(),
            self._timeout_sec,
        )
        self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()
