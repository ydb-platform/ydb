import asyncio
import logging
from typing import (
    Callable,
    Optional,
    List,
)

from .session import (
    QuerySession,
)
from ...retries import (
    RetrySettings,
    retry_operation_async,
)
from ... import convert
from ..._grpc.grpcwrapper import common_utils

logger = logging.getLogger(__name__)


class QuerySessionPool:
    """QuerySessionPool is an object to simplify operations with sessions of Query Service."""

    def __init__(self, driver: common_utils.SupportedDriverType, size: int = 100):
        """
        :param driver: A driver instance
        :param size: Size of session pool
        """

        self._driver = driver
        self._size = size
        self._should_stop = asyncio.Event()
        self._queue = asyncio.Queue()
        self._current_size = 0
        self._waiters = 0
        self._loop = asyncio.get_running_loop()

    async def _create_new_session(self):
        session = QuerySession(self._driver)
        await session.create()
        logger.debug(f"New session was created for pool. Session id: {session._state.session_id}")
        return session

    async def acquire(self) -> QuerySession:
        """Acquire a session from Session Pool.

        :return A QuerySession object.
        """

        if self._should_stop.is_set():
            logger.error("An attempt to take session from closed session pool.")
            raise RuntimeError("An attempt to take session from closed session pool.")

        session = None
        try:
            session = self._queue.get_nowait()
        except asyncio.QueueEmpty:
            pass

        if session is None and self._current_size == self._size:
            queue_get = asyncio.ensure_future(self._queue.get())
            task_stop = asyncio.ensure_future(asyncio.ensure_future(self._should_stop.wait()))
            done, _ = await asyncio.wait((queue_get, task_stop), return_when=asyncio.FIRST_COMPLETED)
            if task_stop in done:
                queue_get.cancel()
                raise RuntimeError("An attempt to take session from closed session pool.")

            task_stop.cancel()
            session = queue_get.result()

        if session is not None:
            if session._state.attached:
                logger.debug(f"Acquired active session from queue: {session._state.session_id}")
                return session
            else:
                self._current_size -= 1
                logger.debug(f"Acquired dead session from queue: {session._state.session_id}")

        logger.debug(f"Session pool is not large enough: {self._current_size} < {self._size}, will create new one.")
        session = await self._create_new_session()
        self._current_size += 1
        return session

    async def release(self, session: QuerySession) -> None:
        """Release a session back to Session Pool."""

        self._queue.put_nowait(session)
        logger.debug("Session returned to queue: %s", session._state.session_id)

    def checkout(self) -> "SimpleQuerySessionCheckoutAsync":
        """Return a Session context manager, that acquires session on enter and releases session on exit."""

        return SimpleQuerySessionCheckoutAsync(self)

    async def retry_operation_async(
        self, callee: Callable, retry_settings: Optional[RetrySettings] = None, *args, **kwargs
    ):
        """Special interface to execute a bunch of commands with session in a safe, retriable way.

        :param callee: A function, that works with session.
        :param retry_settings: RetrySettings object.

        :return: Result sets or exception in case of execution errors.
        """

        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        async def wrapped_callee():
            async with self.checkout() as session:
                return await callee(session, *args, **kwargs)

        return await retry_operation_async(wrapped_callee, retry_settings)

    async def execute_with_retries(
        self,
        query: str,
        parameters: Optional[dict] = None,
        retry_settings: Optional[RetrySettings] = None,
        *args,
        **kwargs,
    ) -> List[convert.ResultSet]:
        """Special interface to execute a one-shot queries in a safe, retriable way.
        Note: this method loads all data from stream before return, do not use this
        method with huge read queries.

        :param query: A query, yql or sql text.
        :param parameters: dict with parameters and YDB types;
        :param retry_settings: RetrySettings object.

        :return: Result sets or exception in case of execution errors.
        """

        retry_settings = RetrySettings() if retry_settings is None else retry_settings

        async def wrapped_callee():
            async with self.checkout() as session:
                it = await session.execute(query, parameters, *args, **kwargs)
                return [result_set async for result_set in it]

        return await retry_operation_async(wrapped_callee, retry_settings)

    async def stop(self):
        self._should_stop.set()

        tasks = []
        while True:
            try:
                session = self._queue.get_nowait()
                tasks.append(session.delete())
            except asyncio.QueueEmpty:
                break

        await asyncio.gather(*tasks)

        logger.debug("All session were deleted.")

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    def __del__(self):
        if self._should_stop.is_set() or self._loop.is_closed():
            return

        self._loop.call_soon(self.stop)


class SimpleQuerySessionCheckoutAsync:
    def __init__(self, pool: QuerySessionPool):
        self._pool = pool
        self._session = None

    async def __aenter__(self) -> QuerySession:
        self._session = await self._pool.acquire()
        return self._session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._pool.release(self._session)
