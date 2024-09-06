import logging
from typing import (
    Optional,
)

from .base import AsyncResponseContextIterator
from ... import issues
from ...query import base
from ...query.transaction import (
    BaseQueryTxContext,
    QueryTxStateEnum,
)

logger = logging.getLogger(__name__)


class QueryTxContextAsync(BaseQueryTxContext):
    async def __aenter__(self) -> "QueryTxContextAsync":
        """
        Enters a context manager and returns a transaction

        :return: A transaction instance
        """
        return self

    async def __aexit__(self, *args, **kwargs):
        """
        Closes a transaction context manager and rollbacks transaction if
        it is not finished explicitly
        """
        await self._ensure_prev_stream_finished()
        if self._tx_state._state == QueryTxStateEnum.BEGINED:
            # It's strictly recommended to close transactions directly
            # by using commit_tx=True flag while executing statement or by
            # .commit() or .rollback() methods, but here we trying to do best
            # effort to avoid useless open transactions
            logger.warning("Potentially leaked tx: %s", self._tx_state.tx_id)
            try:
                await self.rollback()
            except issues.Error:
                logger.warning("Failed to rollback leaked tx: %s", self._tx_state.tx_id)

    async def _ensure_prev_stream_finished(self) -> None:
        if self._prev_stream is not None:
            async with self._prev_stream:
                pass
            self._prev_stream = None

    async def begin(self, settings: Optional[base.QueryClientSettings] = None) -> "QueryTxContextAsync":
        """WARNING: This API is experimental and could be changed.

        Explicitly begins a transaction

        :param settings: A request settings

        :return: None or exception if begin is failed
        """
        await self._begin_call(settings)
        return self

    async def commit(self, settings: Optional[base.QueryClientSettings] = None) -> None:
        """WARNING: This API is experimental and could be changed.

        Calls commit on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A committed transaction or exception if commit is failed
        """
        if self._tx_state._already_in(QueryTxStateEnum.COMMITTED):
            return

        if self._tx_state._state == QueryTxStateEnum.NOT_INITIALIZED:
            self._tx_state._change_state(QueryTxStateEnum.COMMITTED)
            return

        await self._ensure_prev_stream_finished()

        await self._commit_call(settings)

    async def rollback(self, settings: Optional[base.QueryClientSettings] = None) -> None:
        """WARNING: This API is experimental and could be changed.

        Calls rollback on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: A committed transaction or exception if commit is failed
        """
        if self._tx_state._already_in(QueryTxStateEnum.ROLLBACKED):
            return

        if self._tx_state._state == QueryTxStateEnum.NOT_INITIALIZED:
            self._tx_state._change_state(QueryTxStateEnum.ROLLBACKED)
            return

        await self._ensure_prev_stream_finished()

        await self._rollback_call(settings)

    async def execute(
        self,
        query: str,
        parameters: Optional[dict] = None,
        commit_tx: Optional[bool] = False,
        syntax: Optional[base.QuerySyntax] = None,
        exec_mode: Optional[base.QueryExecMode] = None,
        concurrent_result_sets: Optional[bool] = False,
        settings: Optional[base.QueryClientSettings] = None,
    ) -> AsyncResponseContextIterator:
        """WARNING: This API is experimental and could be changed.

        Sends a query to Query Service
        :param query: (YQL or SQL text) to be executed.
        :param parameters: dict with parameters and YDB types;
        :param commit_tx: A special flag that allows transaction commit.
        :param syntax: Syntax of the query, which is a one from the following choises:
         1) QuerySyntax.YQL_V1, which is default;
         2) QuerySyntax.PG.
        :param exec_mode: Exec mode of the query, which is a one from the following choises:
         1) QueryExecMode.EXECUTE, which is default;
         2) QueryExecMode.EXPLAIN;
         3) QueryExecMode.VALIDATE;
         4) QueryExecMode.PARSE.
        :param concurrent_result_sets: A flag to allow YDB mix parts of different result sets. Default is False;

        :return: Iterator with result sets
        """
        await self._ensure_prev_stream_finished()

        stream_it = await self._execute_call(
            query=query,
            commit_tx=commit_tx,
            syntax=syntax,
            exec_mode=exec_mode,
            parameters=parameters,
            concurrent_result_sets=concurrent_result_sets,
        )

        settings = settings if settings is not None else self.session._settings
        self._prev_stream = AsyncResponseContextIterator(
            stream_it,
            lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                tx=self,
                commit_tx=commit_tx,
                settings=settings,
            ),
        )
        return self._prev_stream
