import asyncio

from .reconnector import CoordinationReconnector
from .semaphore import CoordinationSemaphore


class CoordinationSession:
    def __init__(self, driver, path: str, timeout_millis: int = 30000):
        self._driver = driver
        self._path = path
        self._timeout_millis = timeout_millis

        self._reconnector = CoordinationReconnector(
            driver=driver,
            node_path=path,
            timeout_millis=timeout_millis,
        )

        self._req_id = 0
        self._req_id_lock = asyncio.Lock()
        self._closed = False

    async def next_req_id(self) -> int:
        async with self._req_id_lock:
            self._req_id += 1
            return self._req_id

    def semaphore(self, name: str, limit: int = 1) -> CoordinationSemaphore:
        if self._closed:
            raise RuntimeError("CoordinationSession is closed")
        return CoordinationSemaphore(self, name, limit)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        await self._reconnector.stop()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()
