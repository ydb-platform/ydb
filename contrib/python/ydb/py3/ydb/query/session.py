import abc
import enum
import json
import logging
import threading
from typing import (
    Iterable,
    Optional,
    Dict,
    Any,
    Union,
)

from . import base
from .base import QueryExplainResultFormat

from .. import _apis, issues, _utilities
from ..settings import BaseRequestSettings
from ..connection import _RpcState as RpcState
from .._grpc.grpcwrapper import common_utils
from .._grpc.grpcwrapper import ydb_query as _ydb_query
from .._grpc.grpcwrapper import ydb_query_public_types as _ydb_query_public

from .transaction import QueryTxContext

from .._constants import DEFAULT_INITIAL_RESPONSE_TIMEOUT, DEFAULT_LONG_STREAM_TIMEOUT


logger = logging.getLogger(__name__)


class QuerySessionStateEnum(enum.Enum):
    NOT_INITIALIZED = "NOT_INITIALIZED"
    CREATED = "CREATED"
    CLOSED = "CLOSED"


class QuerySessionStateHelper(abc.ABC):
    _VALID_TRANSITIONS = {
        QuerySessionStateEnum.NOT_INITIALIZED: [QuerySessionStateEnum.CREATED],
        QuerySessionStateEnum.CREATED: [QuerySessionStateEnum.CLOSED],
        QuerySessionStateEnum.CLOSED: [],
    }

    _READY_TO_USE = [
        QuerySessionStateEnum.CREATED,
    ]

    @classmethod
    def valid_transition(cls, before: QuerySessionStateEnum, after: QuerySessionStateEnum) -> bool:
        return after in cls._VALID_TRANSITIONS[before]

    @classmethod
    def ready_to_use(cls, state: QuerySessionStateEnum) -> bool:
        return state in cls._READY_TO_USE


class QuerySessionState(base.IQuerySessionState):
    _session_id: Optional[str] = None
    _node_id: Optional[int] = None
    _attached: bool = False
    _settings: Optional[base.QueryClientSettings] = None
    _state: QuerySessionStateEnum = QuerySessionStateEnum.NOT_INITIALIZED

    def __init__(self, settings: base.QueryClientSettings = None):
        self._settings = settings

    def reset(self) -> None:
        self._session_id = None
        self._node_id = None
        self._attached = False

    @property
    def session_id(self) -> Optional[str]:
        return self._session_id

    def set_session_id(self, session_id: str) -> "QuerySessionState":
        self._session_id = session_id
        return self

    @property
    def node_id(self) -> Optional[int]:
        return self._node_id

    def set_node_id(self, node_id: int) -> "QuerySessionState":
        self._node_id = node_id
        return self

    @property
    def attached(self) -> bool:
        return self._attached

    def set_attached(self, attached: bool) -> "QuerySessionState":
        self._attached = attached

    def _check_invalid_transition(self, target: QuerySessionStateEnum) -> None:
        if not QuerySessionStateHelper.valid_transition(self._state, target):
            raise RuntimeError(f"Session could not be moved from {self._state.value} to {target.value}")

    def _change_state(self, target: QuerySessionStateEnum) -> None:
        self._check_invalid_transition(target)
        self._state = target

    def _check_session_ready_to_use(self) -> None:
        if not QuerySessionStateHelper.ready_to_use(self._state):
            raise RuntimeError(f"Session is not ready to use, current state: {self._state.value}")

    def _already_in(self, target) -> bool:
        return self._state == target


def wrapper_create_session(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.CreateSessionResponse,
    session_state: QuerySessionState,
    session: "BaseQuerySession",
) -> "BaseQuerySession":
    message = _ydb_query.CreateSessionResponse.from_proto(response_pb)
    issues._process_response(message.status)
    session_state.set_session_id(message.session_id).set_node_id(message.node_id)
    return session


def wrapper_delete_session(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.DeleteSessionResponse,
    session_state: QuerySessionState,
    session: "BaseQuerySession",
) -> "BaseQuerySession":
    message = _ydb_query.DeleteSessionResponse.from_proto(response_pb)
    issues._process_response(message.status)
    session_state.reset()
    session_state._change_state(QuerySessionStateEnum.CLOSED)
    return session


class BaseQuerySession:
    _driver: common_utils.SupportedDriverType
    _settings: base.QueryClientSettings
    _state: QuerySessionState

    def __init__(self, driver: common_utils.SupportedDriverType, settings: Optional[base.QueryClientSettings] = None):
        self._driver = driver
        self._settings = self._get_client_settings(driver, settings)
        self._state = QuerySessionState(settings)
        self._attach_settings: BaseRequestSettings = (
            BaseRequestSettings()
            .with_operation_timeout(DEFAULT_LONG_STREAM_TIMEOUT)
            .with_cancel_after(DEFAULT_LONG_STREAM_TIMEOUT)
            .with_timeout(DEFAULT_LONG_STREAM_TIMEOUT)
        )

        self._last_query_stats = None

    @property
    def last_query_stats(self):
        return self._last_query_stats

    def _get_client_settings(
        self,
        driver: common_utils.SupportedDriverType,
        settings: Optional[base.QueryClientSettings] = None,
    ) -> base.QueryClientSettings:
        if settings is not None:
            return settings
        if driver._driver_config.query_client_settings is not None:
            return driver._driver_config.query_client_settings
        return base.QueryClientSettings()

    def _create_call(self, settings: Optional[BaseRequestSettings] = None) -> "BaseQuerySession":
        return self._driver(
            _apis.ydb_query.CreateSessionRequest(),
            _apis.QueryService.Stub,
            _apis.QueryService.CreateSession,
            wrap_result=wrapper_create_session,
            wrap_args=(self._state, self),
            settings=settings,
        )

    def _delete_call(self, settings: Optional[BaseRequestSettings] = None) -> "BaseQuerySession":
        return self._driver(
            _apis.ydb_query.DeleteSessionRequest(session_id=self._state.session_id),
            _apis.QueryService.Stub,
            _apis.QueryService.DeleteSession,
            wrap_result=wrapper_delete_session,
            wrap_args=(self._state, self),
            settings=settings,
        )

    def _attach_call(self) -> Iterable[_apis.ydb_query.SessionState]:
        return self._driver(
            _apis.ydb_query.AttachSessionRequest(session_id=self._state.session_id),
            _apis.QueryService.Stub,
            _apis.QueryService.AttachSession,
            settings=self._attach_settings,
        )

    def _execute_call(
        self,
        query: str,
        parameters: dict = None,
        commit_tx: bool = False,
        syntax: base.QuerySyntax = None,
        exec_mode: base.QueryExecMode = None,
        stats_mode: Optional[base.QueryStatsMode] = None,
        concurrent_result_sets: bool = False,
        settings: Optional[BaseRequestSettings] = None,
    ) -> Iterable[_apis.ydb_query.ExecuteQueryResponsePart]:
        self._last_query_stats = None

        request = base.create_execute_query_request(
            query=query,
            parameters=parameters,
            commit_tx=commit_tx,
            session_id=self._state.session_id,
            tx_mode=None,
            tx_id=None,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            concurrent_result_sets=concurrent_result_sets,
        )

        return self._driver(
            request.to_proto(),
            _apis.QueryService.Stub,
            _apis.QueryService.ExecuteQuery,
            settings=settings,
        )


class QuerySession(BaseQuerySession):
    """Session object for Query Service. It is not recommended to control
    session's lifecycle manually - use a QuerySessionPool is always a better choise.
    """

    _stream = None

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
            if first_response.status != issues.StatusCode.SUCCESS:
                raise RuntimeError("Failed to attach session")
        except Exception as e:
            self._state.reset()
            status_stream.cancel()
            raise e

        self._state.set_attached(True)
        self._state._change_state(QuerySessionStateEnum.CREATED)

        threading.Thread(
            target=self._check_session_status_loop,
            args=(status_stream,),
            name="attach stream thread",
            daemon=True,
        ).start()

    def _check_session_status_loop(self, status_stream: _utilities.SyncResponseIterator) -> None:
        try:
            for status in status_stream:
                if status.status != issues.StatusCode.SUCCESS:
                    self._state.reset()
                    self._state._change_state(QuerySessionStateEnum.CLOSED)
        except Exception:
            if not self._state._already_in(QuerySessionStateEnum.CLOSED):
                self._state.reset()
                self._state._change_state(QuerySessionStateEnum.CLOSED)

    def delete(self, settings: Optional[BaseRequestSettings] = None) -> None:
        """Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        if self._state._already_in(QuerySessionStateEnum.CLOSED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CLOSED)
        self._delete_call(settings=settings)
        self._stream.cancel()

    def create(self, settings: Optional[BaseRequestSettings] = None) -> "QuerySession":
        """Creates a Session of Query Service on server side and attaches it.

        :return: QuerySession object.
        """
        if self._state._already_in(QuerySessionStateEnum.CREATED):
            return

        self._state._check_invalid_transition(QuerySessionStateEnum.CREATED)

        self._create_call(settings=settings)
        self._attach()

        return self

    def transaction(self, tx_mode: Optional[base.BaseQueryTxMode] = None) -> QueryTxContext:
        """Creates a transaction context manager with specified transaction mode.

        :param tx_mode: Transaction mode, which is a one from the following choices:
         1) QuerySerializableReadWrite() which is default mode;
         2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
         3) QuerySnapshotReadOnly();
         4) QueryStaleReadOnly().

        :return transaction context manager.

        """
        self._state._check_session_ready_to_use()

        tx_mode = tx_mode if tx_mode else _ydb_query_public.QuerySerializableReadWrite()

        return QueryTxContext(
            self._driver,
            self._state,
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

        :return: Iterator with result sets
        """
        self._state._check_session_ready_to_use()

        stream_it = self._execute_call(
            query=query,
            parameters=parameters,
            commit_tx=True,
            syntax=syntax,
            exec_mode=exec_mode,
            stats_mode=stats_mode,
            concurrent_result_sets=concurrent_result_sets,
            settings=settings,
        )

        return base.SyncResponseContextIterator(
            stream_it,
            lambda resp: base.wrap_execute_query_response(
                rpc_state=None,
                response_pb=resp,
                session_state=self._state,
                session=self,
                settings=self._settings,
            ),
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
