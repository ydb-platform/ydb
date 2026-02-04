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
from ...query.session import BaseQuerySession

from ..._constants import DEFAULT_INITIAL_RESPONSE_TIMEOUT

import logging

logger = logging.getLogger(__name__)


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
            issues._process_response(first_response)
        except Exception as e:
            self._invalidate()
            raise e

        self._loop.create_task(self._check_session_status_loop(), name="check session status task")

    async def _check_session_status_loop(self) -> None:
        try:
            async for status in self._status_stream:
                issues._process_response(status)
            logger.debug("Attach stream closed, session_id: %s", self._session_id)
        except Exception as e:
            logger.debug("Attach stream error: %s, session_id: %s", e, self._session_id)
            self._invalidate()

    async def delete(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        if self._closed:
            return

        if self._session_id:
            try:
                await self._delete_call(settings=settings)
            except Exception:
                pass

        self._invalidate()

    async def create(self, settings: Optional[BaseRequestSettings] = None) -> "QuerySession":
        """Creates a Session of Query Service on server side and attaches it.

        :return: QuerySession object.
        """
        if self.is_active:
            return self

        if self._closed:
            raise RuntimeError("Session is already closed")

        await self._create_call(settings=settings)
        await self._attach()

        return self

    def transaction(self, tx_mode=None) -> QueryTxContext:
        self._check_session_ready_to_use()
        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()

        return QueryTxContext(
            self._driver,
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
        schema_inclusion_mode: Optional[base.QuerySchemaInclusionMode] = None,
        result_set_format: Optional[base.QueryResultSetFormat] = None,
        arrow_format_settings: Optional[base.ArrowFormatSettings] = None,
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
        :param schema_inclusion_mode: Schema inclusion mode for result sets:
         1) QuerySchemaInclusionMode.ALWAYS, which is default;
         2) QuerySchemaInclusionMode.FIRST_ONLY.
        :param result_set_format: Format of the result sets:
         1) QueryResultSetFormat.VALUE, which is default;
         2) QueryResultSetFormat.ARROW.
        :param arrow_format_settings: Settings for Arrow format when result_set_format is ARROW.

        :return: Iterator with result sets
        """
        self._check_session_ready_to_use()

        stream_it = await self._execute_call(
            query=query,
            parameters=parameters,
            commit_tx=True,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            schema_inclusion_mode=schema_inclusion_mode,
            result_set_format=result_set_format,
            arrow_format_settings=arrow_format_settings,
            concurrent_result_sets=concurrent_result_sets,
            settings=settings,
        )

        return AsyncResponseContextIterator(
            it=stream_it,
            wrapper=lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                session=self,
                settings=self._settings,
            ),
            on_error=self._on_execute_stream_error,
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
