import logging
from typing import (
    Optional,
)

from .base import AsyncResponseContextIterator
from ... import issues
from ...settings import BaseRequestSettings
from ...query import base
from ...query.transaction import (
    BaseQueryTxContext,
    QueryTxStateEnum,
)
from ..._errors import stream_error_converter

logger = logging.getLogger(__name__)


class QueryTxContext(BaseQueryTxContext):
    def __init__(self, driver, session_state, session, tx_mode):
        """
        An object that provides a simple transaction context manager that allows statements execution
        in a transaction. You don't have to open transaction explicitly, because context manager encapsulates
        transaction control logic, and opens new transaction if:

        1) By explicit .begin() method;
        2) On execution of a first statement, which is strictly recommended method, because that avoids useless round trip

        This context manager is not thread-safe, so you should not manipulate on it concurrently.

        :param driver: A driver instance
        :param session_state: A state of session
        :param tx_mode: Transaction mode, which is a one from the following choices:
         1) QuerySerializableReadWrite() which is default mode;
         2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
         3) QuerySnapshotReadOnly();
         4) QueryStaleReadOnly().
        """
        super().__init__(driver, session_state, session, tx_mode)
        self._init_callback_handler(base.CallbackHandlerMode.ASYNC)

    async def __aenter__(self) -> "QueryTxContext":
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
        if self._tx_state._state == QueryTxStateEnum.BEGINED and self._external_error is None:
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

    async def begin(self, settings: Optional[BaseRequestSettings] = None) -> "QueryTxContext":
        """Explicitly begins a transaction

        :param settings: An additional request settings BaseRequestSettings;

        :return: None or exception if begin is failed
        """
        await self._begin_call(settings)
        return self

    async def commit(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Calls commit on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: An additional request settings BaseRequestSettings;

        :return: A committed transaction or exception if commit is failed
        """
        self._check_external_error_set()

        if self._tx_state._should_skip(QueryTxStateEnum.COMMITTED):
            return

        if self._tx_state._state == QueryTxStateEnum.NOT_INITIALIZED:
            self._tx_state._change_state(QueryTxStateEnum.COMMITTED)
            return

        await self._ensure_prev_stream_finished()

        try:
            await self._execute_callbacks_async(base.TxEvent.BEFORE_COMMIT)
            await self._commit_call(settings)
            await self._execute_callbacks_async(base.TxEvent.AFTER_COMMIT, exc=None)
        except BaseException as e:
            await self._execute_callbacks_async(base.TxEvent.AFTER_COMMIT, exc=e)
            raise e

    async def rollback(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Calls rollback on a transaction if it is open otherwise is no-op. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: An additional request settings BaseRequestSettings;

        :return: A committed transaction or exception if commit is failed
        """
        self._check_external_error_set()

        if self._tx_state._should_skip(QueryTxStateEnum.ROLLBACKED):
            return

        if self._tx_state._state == QueryTxStateEnum.NOT_INITIALIZED:
            self._tx_state._change_state(QueryTxStateEnum.ROLLBACKED)
            return

        await self._ensure_prev_stream_finished()

        try:
            await self._execute_callbacks_async(base.TxEvent.BEFORE_ROLLBACK)
            await self._rollback_call(settings)
            await self._execute_callbacks_async(base.TxEvent.AFTER_ROLLBACK, exc=None)
        except BaseException as e:
            await self._execute_callbacks_async(base.TxEvent.AFTER_ROLLBACK, exc=e)
            raise e

    async def execute(
        self,
        query: str,
        parameters: Optional[dict] = None,
        commit_tx: Optional[bool] = False,
        syntax: Optional[base.QuerySyntax] = None,
        exec_mode: Optional[base.QueryExecMode] = None,
        concurrent_result_sets: Optional[bool] = False,
        settings: Optional[BaseRequestSettings] = None,
        *,
        stats_mode: Optional[base.QueryStatsMode] = None,
    ) -> AsyncResponseContextIterator:
        """Sends a query to Query Service

        :param query: (YQL or SQL text) to be executed.
        :param parameters: dict with parameters and YDB types;
        :param commit_tx: A special flag that allows transaction commit.
        :param syntax: Syntax of the query, which is a one from the following choices:
         1) QuerySyntax.YQL_V1, which is default;
         2) QuerySyntax.PG.
        :param exec_mode: Exec mode of the query, which is a one from the following choices:
         1) QueryExecMode.EXECUTE, which is default;
         2) QueryExecMode.EXPLAIN;
         3) QueryExecMode.VALIDATE;
         4) QueryExecMode.PARSE.
        :param concurrent_result_sets: A flag to allow YDB mix parts of different result sets. Default is False;
        :param stats_mode: Mode of query statistics to gather, which is a one from the following choices:
         1) QueryStatsMode:NONE, which is default;
         2) QueryStatsMode.BASIC;
         3) QueryStatsMode.FULL;
         4) QueryStatsMode.PROFILE;

        :return: Iterator with result sets
        """
        await self._ensure_prev_stream_finished()

        stream_it = await self._execute_call(
            query=query,
            parameters=parameters,
            commit_tx=commit_tx,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            concurrent_result_sets=concurrent_result_sets,
            settings=settings,
        )

        self._prev_stream = AsyncResponseContextIterator(
            it=stream_it,
            wrapper=lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                session_state=self._session_state,
                tx=self,
                commit_tx=commit_tx,
                settings=self.session._settings,
            ),
            error_converter=stream_error_converter,
        )
        return self._prev_stream
