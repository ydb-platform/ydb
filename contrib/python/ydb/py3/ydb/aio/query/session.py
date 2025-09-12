import asyncio
import json

from typing import (
    Optional,
    Dict,
    Any,
    Union,
)

from .base import AsyncResponseContextIterator
from .transaction import QueryTxContext
from .. import _utilities
from ... import issues
from ...settings import BaseRequestSettings
from ..._grpc.grpcwrapper import common_utils
from ..._grpc.grpcwrapper import ydb_query_public_types as _ydb_query_public

from ...query import base
from ...query.session import (
    BaseQuerySession,
    QuerySessionStateEnum,
)

from ..._constants import DEFAULT_INITIAL_RESPONSE_TIMEOUT
from ..._errors import stream_error_converter


class QuerySession(BaseQuerySession):
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
        super(QuerySession, self).__init__(driver, settings)
        self._loop = loop if loop is not None else asyncio.get_running_loop()

    async def _attach(self) -> None:
        self._stream = await self._attach_call()
        self._status_stream = _utilities.AsyncResponseIterator(
            self._stream,
            lambda response: common_utils.ServerStatus.from_proto(response),
        )

        try:
            first_response = await _utilities.get_first_message_with_timeout(
                self._status_stream,
                DEFAULT_INITIAL_RESPONSE_TIMEOUT,
            )
            if first_response.status != issues.StatusCode.SUCCESS:
                raise RuntimeError("Failed to attach session")
        except Exception as e:
            self._state.reset()
            self._status_stream.cancel()
            raise e

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

    async def delete(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        if self._state._already_in(QuerySessionStateEnum.CLOSED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CLOSED)
        await self._delete_call(settings=settings)
        self._stream.cancel()

    async def create(self, settings: Optional[BaseRequestSettings] = None) -> "QuerySession":
        """Creates a Session of Query Service on server side and attaches it.

        :return: QuerySession object.
        """
        if self._state._already_in(QuerySessionStateEnum.CREATED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CREATED)
        await self._create_call(settings=settings)
        await self._attach()

        return self

    def transaction(self, tx_mode=None) -> QueryTxContext:
        self._state._check_session_ready_to_use()
        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()

        return QueryTxContext(
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
        settings: Optional[BaseRequestSettings] = None,
        *,
        stats_mode: Optional[base.QueryStatsMode] = None,
    ) -> AsyncResponseContextIterator:
        """Sends a query to Query Service

        :param query: (YQL or SQL text) to be executed.
        :param syntax: Syntax of the query, which is a one from the following choices:
         1) QuerySyntax.YQL_V1, which is default;
         2) QuerySyntax.PG.
        :param parameters: dict with parameters and YDB types;
        :param concurrent_result_sets: A flag to allow YDB mix parts of different result sets. Default is False;
        :param stats_mode: Mode of query statistics to gather, which is a one from the following choices:
         1) QueryStatsMode:NONE, which is default;
         2) QueryStatsMode.BASIC;
         3) QueryStatsMode.FULL;
         4) QueryStatsMode.PROFILE;

        :return: Iterator with result sets
        """
        self._state._check_session_ready_to_use()

        stream_it = await self._execute_call(
            query=query,
            parameters=parameters,
            commit_tx=True,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            concurrent_result_sets=concurrent_result_sets,
            settings=settings,
        )

        return AsyncResponseContextIterator(
            it=stream_it,
            wrapper=lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                session_state=self._state,
                session=self,
                settings=self._settings,
            ),
            error_converter=stream_error_converter,
        )

    async def explain(
        self,
        query: str,
        parameters: Optional[dict] = None,
        result_format: base.QueryExplainResultFormat = base.QueryExplainResultFormat.STR,
    ) -> Union[str, Dict[str, Any]]:
        """Explains query result
        :param query: YQL or SQL query.
        :param parameters: dict with parameters and YDB types;
        :param result_format: Return format: string or dict.
        :return: Parsed query plan.
        """

        res = await self.execute(query, parameters, exec_mode=base.QueryExecMode.EXPLAIN)

        # it needs to read result sets for set last_query_stats as sideeffect
        async for _ in res:
            pass

        plan = self.last_query_stats.query_plan

        if result_format == base.QueryExplainResultFormat.DICT:
            plan = json.loads(plan)

        return plan
