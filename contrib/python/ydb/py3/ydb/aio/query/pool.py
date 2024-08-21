import logging
from typing import (
    Callable,
    Optional,
    List,
)

from .session import (
    QuerySessionAsync,
)
from ...retries import (
    RetrySettings,
    retry_operation_async,
)
from ... import convert
from ..._grpc.grpcwrapper import common_utils

logger = logging.getLogger(__name__)


class QuerySessionPoolAsync:
    """QuerySessionPoolAsync is an object to simplify operations with sessions of Query Service."""

    def __init__(self, driver: common_utils.SupportedDriverType):
        """
        :param driver: A driver instance
        """

        logger.warning("QuerySessionPoolAsync is an experimental API, which could be changed.")
        self._driver = driver

    def checkout(self) -> "SimpleQuerySessionCheckoutAsync":
        """WARNING: This API is experimental and could be changed.
        Return a Session context manager, that opens session on enter and closes session on exit.
        """

        return SimpleQuerySessionCheckoutAsync(self)

    async def retry_operation_async(
        self, callee: Callable, retry_settings: Optional[RetrySettings] = None, *args, **kwargs
    ):
        """WARNING: This API is experimental and could be changed.
        Special interface to execute a bunch of commands with session in a safe, retriable way.

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
        """WARNING: This API is experimental and could be changed.
        Special interface to execute a one-shot queries in a safe, retriable way.
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

    async def stop(self, timeout=None):
        pass  # TODO: implement

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()


class SimpleQuerySessionCheckoutAsync:
    def __init__(self, pool: QuerySessionPoolAsync):
        self._pool = pool
        self._session = QuerySessionAsync(pool._driver)

    async def __aenter__(self) -> QuerySessionAsync:
        await self._session.create()
        return self._session

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self._session.delete()
