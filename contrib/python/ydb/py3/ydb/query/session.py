import abc
import json
import logging
import threading
from typing import (
    Awaitable,
    Generic,
    Iterable,
    Optional,
    Dict,
    Any,
    TYPE_CHECKING,
    Union,
    overload,
)

from . import base
from .base import QueryExplainResultFormat

from .. import _apis, issues, _utilities
from ..settings import BaseRequestSettings
from ..connection import _RpcState as RpcState, EndpointKey
from .._grpc.grpcwrapper import common_utils
from .._grpc.grpcwrapper import ydb_query as _ydb_query
from .._grpc.grpcwrapper import ydb_query_public_types as _ydb_query_public
from .._typing import DriverT, GrpcStreamCall, SupportedDriverType

from .transaction import QueryTxContext

from .._constants import DEFAULT_INITIAL_RESPONSE_TIMEOUT, DEFAULT_LONG_STREAM_TIMEOUT

if TYPE_CHECKING:
    from ..driver import Driver as SyncDriver
    from ..aio.driver import Driver as AsyncDriver


logger = logging.getLogger(__name__)


def wrapper_create_session(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.CreateSessionResponse,
    session: "BaseQuerySession",
) -> "BaseQuerySession":
    message = _ydb_query.CreateSessionResponse.from_proto(response_pb)
    issues._process_response(message.status)
    session._session_id = message.session_id
    session._node_id = message.node_id
    return session


def wrapper_delete_session(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.DeleteSessionResponse,
    session: "BaseQuerySession",
) -> "BaseQuerySession":
    message = _ydb_query.DeleteSessionResponse.from_proto(response_pb)
    issues._process_response(message.status)
    session._closed = True
    return session


class BaseQuerySession(abc.ABC, Generic[DriverT]):
    """Generic session - parametrized by driver type for proper typing."""

    _driver: DriverT
    _settings: base.QueryClientSettings
    _stream: Optional[GrpcStreamCall[_apis.ydb_query.SessionState]] = None

    # Session data
    _session_id: Optional[str] = None
    _node_id: Optional[int] = None
    _closed: bool = False

    def __init__(self, driver: DriverT, settings: Optional[base.QueryClientSettings] = None):
        self._driver = driver
        self._settings = self._get_client_settings(driver, settings)
        self._attach_settings: BaseRequestSettings = (
            BaseRequestSettings()
            .with_operation_timeout(DEFAULT_LONG_STREAM_TIMEOUT)
            .with_cancel_after(DEFAULT_LONG_STREAM_TIMEOUT)
            .with_timeout(DEFAULT_LONG_STREAM_TIMEOUT)
        )

        self._last_query_stats = None

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    @property
    def node_id(self) -> Optional[int]:
        return self._node_id

    @property
    def is_active(self) -> bool:
        return self._session_id is not None and not self._closed

    @property
    def _endpoint_key(self) -> Optional[EndpointKey]:
        if self._node_id is None:
            return None
        return EndpointKey(endpoint=None, node_id=self._node_id)

    @property
    def is_closed(self) -> bool:
        return self._closed

    @property
    def last_query_stats(self):
        return self._last_query_stats

    def _get_client_settings(
        self,
        driver: SupportedDriverType,
        settings: Optional[base.QueryClientSettings] = None,
    ) -> base.QueryClientSettings:
        if settings is not None:
            return settings
        if driver._driver_config.query_client_settings is not None:
            return driver._driver_config.query_client_settings
        return base.QueryClientSettings()

    def _check_session_ready_to_use(self) -> None:
        if not self.is_active:
            raise RuntimeError(f"Session is not active, session_id: {self._session_id}, closed: {self._closed}")

    def _invalidate(self) -> None:
        if self._closed:
            return
        self._closed = True

        if self._stream is not None:
            try:
                self._stream.cancel()
            except Exception:
                pass

    def _on_execute_stream_error(self, e: Exception) -> None:
        if isinstance(e, issues.DeadlineExceed):
            self._invalidate()

    # Overloads for _create_call
    @overload
    def _create_call(
        self: "BaseQuerySession[SyncDriver]", settings: Optional[BaseRequestSettings] = None
    ) -> "BaseQuerySession[SyncDriver]":
        ...

    @overload
    def _create_call(
        self: "BaseQuerySession[AsyncDriver]", settings: Optional[BaseRequestSettings] = None
    ) -> Awaitable["BaseQuerySession[AsyncDriver]"]:
        ...

    def _create_call(
        self, settings: Optional[BaseRequestSettings] = None
    ) -> "Union[BaseQuerySession[Any], Awaitable[BaseQuerySession[Any]]]":
        """Create session. Returns Awaitable in async context."""
        return self._driver(
            _apis.ydb_query.CreateSessionRequest(),
            _apis.QueryService.Stub,
            _apis.QueryService.CreateSession,
            wrap_result=wrapper_create_session,
            wrap_args=(self,),
            settings=settings,
        )

    # Overloads for _delete_call
    @overload
    def _delete_call(
        self: "BaseQuerySession[SyncDriver]", settings: Optional[BaseRequestSettings] = None
    ) -> "BaseQuerySession[SyncDriver]":
        ...

    @overload
    def _delete_call(
        self: "BaseQuerySession[AsyncDriver]", settings: Optional[BaseRequestSettings] = None
    ) -> Awaitable["BaseQuerySession[AsyncDriver]"]:
        ...

    def _delete_call(
        self, settings: Optional[BaseRequestSettings] = None
    ) -> "Union[BaseQuerySession[Any], Awaitable[BaseQuerySession[Any]]]":
        """Delete session. Returns Awaitable in async context."""
        return self._driver(
            _apis.ydb_query.DeleteSessionRequest(session_id=self._session_id),
            _apis.QueryService.Stub,
            _apis.QueryService.DeleteSession,
            wrap_result=wrapper_delete_session,
            wrap_args=(self,),
            settings=settings,
            preferred_endpoint=self._endpoint_key,
        )

    # Overloads for _attach_call
    @overload
    def _attach_call(
        self: "BaseQuerySession[SyncDriver]",
    ) -> GrpcStreamCall[_apis.ydb_query.SessionState]:
        ...

    @overload
    def _attach_call(
        self: "BaseQuerySession[AsyncDriver]",
    ) -> Awaitable[GrpcStreamCall[_apis.ydb_query.SessionState]]:
        ...

    def _attach_call(
        self,
    ) -> Union[GrpcStreamCall[_apis.ydb_query.SessionState], Awaitable[GrpcStreamCall[_apis.ydb_query.SessionState]]]:
        """Attach to session. Returns Awaitable in async context."""
        return self._driver(
            _apis.ydb_query.AttachSessionRequest(session_id=self._session_id),
            _apis.QueryService.Stub,
            _apis.QueryService.AttachSession,
            settings=self._attach_settings,
            preferred_endpoint=self._endpoint_key,
        )

    # Overloads for _execute_call
    @overload
    def _execute_call(
        self: "BaseQuerySession[SyncDriver]",
        query: str,
        parameters: Optional[dict] = None,
        commit_tx: bool = False,
        syntax: Optional[base.QuerySyntax] = None,
        exec_mode: Optional[base.QueryExecMode] = None,
        stats_mode: Optional[base.QueryStatsMode] = None,
        schema_inclusion_mode: Optional[base.QuerySchemaInclusionMode] = None,
        result_set_format: Optional[base.QueryResultSetFormat] = None,
        arrow_format_settings: Optional[base.ArrowFormatSettings] = None,
        concurrent_result_sets: bool = False,
        settings: Optional[BaseRequestSettings] = None,
    ) -> Iterable[_apis.ydb_query.ExecuteQueryResponsePart]:
        ...

    @overload
    def _execute_call(
        self: "BaseQuerySession[AsyncDriver]",
        query: str,
        parameters: Optional[dict] = None,
        commit_tx: bool = False,
        syntax: Optional[base.QuerySyntax] = None,
        exec_mode: Optional[base.QueryExecMode] = None,
        stats_mode: Optional[base.QueryStatsMode] = None,
        schema_inclusion_mode: Optional[base.QuerySchemaInclusionMode] = None,
        result_set_format: Optional[base.QueryResultSetFormat] = None,
        arrow_format_settings: Optional[base.ArrowFormatSettings] = None,
        concurrent_result_sets: bool = False,
        settings: Optional[BaseRequestSettings] = None,
    ) -> Awaitable[Iterable[_apis.ydb_query.ExecuteQueryResponsePart]]:
        ...

    def _execute_call(
        self,
        query: str,
        parameters: Optional[dict] = None,
        commit_tx: bool = False,
        syntax: Optional[base.QuerySyntax] = None,
        exec_mode: Optional[base.QueryExecMode] = None,
        stats_mode: Optional[base.QueryStatsMode] = None,
        schema_inclusion_mode: Optional[base.QuerySchemaInclusionMode] = None,
        result_set_format: Optional[base.QueryResultSetFormat] = None,
        arrow_format_settings: Optional[base.ArrowFormatSettings] = None,
        concurrent_result_sets: bool = False,
        settings: Optional[BaseRequestSettings] = None,
    ) -> Union[
        Iterable[_apis.ydb_query.ExecuteQueryResponsePart],
        Awaitable[Iterable[_apis.ydb_query.ExecuteQueryResponsePart]],
    ]:
        self._last_query_stats = None

        if self._session_id is None:
            raise RuntimeError("Session is not initialized")

        request = base.create_execute_query_request(
            query=query,
            parameters=parameters,
            commit_tx=commit_tx,
            session_id=self._session_id,
            tx_mode=None,
            tx_id=None,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            schema_inclusion_mode=schema_inclusion_mode,
            result_set_format=result_set_format,
            arrow_format_settings=arrow_format_settings,
            concurrent_result_sets=concurrent_result_sets,
        )

        return self._driver(
            request.to_proto(),
            _apis.QueryService.Stub,
            _apis.QueryService.ExecuteQuery,
            settings=settings,
            preferred_endpoint=self._endpoint_key,
        )


class QuerySession(BaseQuerySession["SyncDriver"]):
    """Session object for Query Service. It is not recommended to control
    session's lifecycle manually - use a QuerySessionPool is always a better choice.
    """

    def __init__(self, driver: "SyncDriver", settings: Optional[base.QueryClientSettings] = None):
        super().__init__(driver, settings)

    def _attach(self, first_resp_timeout: int = DEFAULT_INITIAL_RESPONSE_TIMEOUT) -> None:
        self._stream = self._attach_call()
        status_stream = _utilities.SyncResponseIterator(
            self._stream,
            lambda response: common_utils.ServerStatus.from_proto(response),
        )

        try:
            first_response = _utilities.get_first_message_with_timeout(
                status_stream,
                first_resp_timeout,
            )
            issues._process_response(first_response)
        except Exception as e:
            self._invalidate()
            raise e

        threading.Thread(
            target=self._check_session_status_loop,
            args=(status_stream,),
            name="attach stream thread",
            daemon=True,
        ).start()

    def _check_session_status_loop(self, status_stream: _utilities.SyncResponseIterator) -> None:
        try:
            for status in status_stream:
                issues._process_response(status)
            logger.debug("Attach stream closed, session_id: %s", self._session_id)
        except Exception as e:
            logger.debug("Attach stream error: %s, session_id: %s", e, self._session_id)
            self._invalidate()

    def delete(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        if self._closed:
            return

        if self._session_id:
            try:
                self._delete_call(settings=settings)
            except Exception:
                pass

        self._invalidate()

    def create(self, settings: Optional[BaseRequestSettings] = None) -> "QuerySession":
        """Creates a Session of Query Service on server side and attaches it.

        :return: QuerySession object.
        """
        if self.is_active:
            return self

        if self._closed:
            raise RuntimeError("Session is already closed.")

        self._create_call(settings=settings)
        self._attach()

        return self

    def transaction(self, tx_mode: Optional[base.BaseQueryTxMode] = None) -> QueryTxContext:
        """Creates a transaction context manager with specified transaction mode.

        :param tx_mode: Transaction mode, which is a one from the following choices:
         1) QuerySerializableReadWrite() which is default mode;
         2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
         3) QuerySnapshotReadOnly();
         4) QuerySnapshotReadWrite();
         5) QueryStaleReadOnly().

        :return transaction context manager.

        """
        self._check_session_ready_to_use()

        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()

        return QueryTxContext(
            self._driver,
            self,
            tx_mode,
        )

    def execute(
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
    ) -> base.SyncResponseContextIterator:
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

        stream_it = self._execute_call(
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

        return base.SyncResponseContextIterator(
            stream_it,
            lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                session=self,
                settings=self._settings,
            ),
            on_error=self._on_execute_stream_error,
        )

    def explain(
        self,
        query: str,
        parameters: dict = None,
        *,
        result_format: QueryExplainResultFormat = QueryExplainResultFormat.STR,
    ) -> Union[str, Dict[str, Any]]:
        """Explains query result
        :param query: YQL or SQL query.
        :param parameters: dict with parameters and YDB types;
        :param result_format: Return format: string or dict.
        :return: Parsed query plan.
        """

        res = self.execute(query, parameters, exec_mode=base.QueryExecMode.EXPLAIN)

        # is needs to read result sets for set last_query_stats as sideeffect
        for _ in res:
            pass

        plan = self.last_query_stats.query_plan
        if result_format == QueryExplainResultFormat.DICT:
            plan = json.loads(plan)

        return plan
