import asyncio

from typing import (
    Optional,
)

from .base import AsyncResponseContextIterator
from .transaction import QueryTxContextAsync
from .. import _utilities
from ... import issues
from ..._grpc.grpcwrapper import common_utils
from ..._grpc.grpcwrapper import ydb_query_public_types as _ydb_query_public

from ...query import base
from ...query.session import (
    BaseQuerySession,
    QuerySessionStateEnum,
)


class QuerySessionAsync(BaseQuerySession):
    """Session object for Query Service. It is not recommended to control
    session's lifecycle manually - use a QuerySessionPool is always a better choise.
    """

    _loop: asyncio.AbstractEventLoop
    _status_stream: _utilities.AsyncResponseIterator = None

    def __init__(
        self,
        driver: common_utils.SupportedDriverType,
        settings: Optional[base.QueryClientSettings] = None,
        loop: asyncio.AbstractEventLoop = None,
    ):
        super(QuerySessionAsync, self).__init__(driver, settings)
        self._loop = loop if loop is not None else asyncio.get_running_loop()

    async def _attach(self) -> None:
        self._stream = await self._attach_call()
        self._status_stream = _utilities.AsyncResponseIterator(
            self._stream,
            lambda response: common_utils.ServerStatus.from_proto(response),
        )

        first_response = await self._status_stream.next()
        if first_response.status != issues.StatusCode.SUCCESS:
            pass

        self._state.set_attached(True)
        self._state._change_state(QuerySessionStateEnum.CREATED)

        self._loop.create_task(self._check_session_status_loop(), name="check session status task")

    async def _check_session_status_loop(self) -> None:
        try:
            async for status in self._status_stream:
                if status.status != issues.StatusCode.SUCCESS:
                    self._state.reset()
                    self._state._change_state(QuerySessionStateEnum.CLOSED)
        except Exception:
            if not self._state._already_in(QuerySessionStateEnum.CLOSED):
                self._state.reset()
                self._state._change_state(QuerySessionStateEnum.CLOSED)

    async def delete(self) -> None:
        """WARNING: This API is experimental and could be changed.

        Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        if self._state._already_in(QuerySessionStateEnum.CLOSED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CLOSED)
        await self._delete_call()
        self._stream.cancel()

    async def create(self) -> "QuerySessionAsync":
        """WARNING: This API is experimental and could be changed.

        Creates a Session of Query Service on server side and attaches it.

        :return: QuerySessionSync object.
        """
        if self._state._already_in(QuerySessionStateEnum.CREATED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CREATED)
        await self._create_call()
        await self._attach()

        return self

    def transaction(self, tx_mode=None) -> QueryTxContextAsync:
        self._state._check_session_ready_to_use()
        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()

        return QueryTxContextAsync(
            self._driver,
            self._state,
            self,
            tx_mode,
        )

    async def execute(
        self,
        query: str,
        parameters: dict = None,
        syntax: base.QuerySyntax = None,
        exec_mode: base.QueryExecMode = None,
        concurrent_result_sets: bool = False,
    ) -> AsyncResponseContextIterator:
        """WARNING: This API is experimental and could be changed.

        Sends a query to Query Service
        :param query: (YQL or SQL text) to be executed.
        :param syntax: Syntax of the query, which is a one from the following choises:
         1) QuerySyntax.YQL_V1, which is default;
         2) QuerySyntax.PG.
        :param parameters: dict with parameters and YDB types;
        :param concurrent_result_sets: A flag to allow YDB mix parts of different result sets. Default is False;

        :return: Iterator with result sets
        """
        self._state._check_session_ready_to_use()

        stream_it = await self._execute_call(
            query=query,
            commit_tx=True,
            syntax=syntax,
            exec_mode=exec_mode,
            parameters=parameters,
            concurrent_result_sets=concurrent_result_sets,
        )

        return AsyncResponseContextIterator(
            stream_it,
            lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                settings=self._settings,
            ),
        )
