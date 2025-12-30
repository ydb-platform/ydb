import abc
import asyncio
import enum
import functools
from collections import defaultdict

import typing
from typing import (
    Optional,
)

from .._grpc.grpcwrapper import ydb_query
from .._grpc.grpcwrapper.ydb_query_public_types import (
    BaseQueryTxMode,
)
from ..connection import _RpcState as RpcState
from .. import convert
from .. import issues
from .. import _utilities
from .. import _apis

from ydb._topic_common.common import CallFromSyncToAsync, _get_shared_event_loop
from ydb._grpc.grpcwrapper.common_utils import to_thread


if typing.TYPE_CHECKING:
    from .transaction import BaseQueryTxContext
    from .session import BaseQuerySession


class QuerySyntax(enum.IntEnum):
    UNSPECIFIED = 0
    YQL_V1 = 1
    PG = 2


class QueryExecMode(enum.IntEnum):
    UNSPECIFIED = 0
    PARSE = 10
    VALIDATE = 20
    EXPLAIN = 30
    EXECUTE = 50


class QueryExplainResultFormat(enum.Enum):
    STR = 0
    DICT = 10


class QueryStatsMode(enum.IntEnum):
    UNSPECIFIED = 0
    NONE = 10
    BASIC = 20
    FULL = 30
    PROFILE = 40


class SyncResponseContextIterator(_utilities.SyncResponseIterator):
    def __enter__(self) -> "SyncResponseContextIterator":
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        #  To close stream on YDB it is necessary to scroll through it to the end
        for _ in self:
            pass


class QueryClientSettings:
    def __init__(self):
        self._native_datetime_in_result_sets = True
        self._native_date_in_result_sets = True
        self._native_json_in_result_sets = True
        self._native_interval_in_result_sets = True
        self._native_timestamp_in_result_sets = True

    def with_native_timestamp_in_result_sets(self, enabled: bool) -> "QueryClientSettings":
        self._native_timestamp_in_result_sets = enabled
        return self

    def with_native_interval_in_result_sets(self, enabled: bool) -> "QueryClientSettings":
        self._native_interval_in_result_sets = enabled
        return self

    def with_native_json_in_result_sets(self, enabled: bool) -> "QueryClientSettings":
        self._native_json_in_result_sets = enabled
        return self

    def with_native_date_in_result_sets(self, enabled: bool) -> "QueryClientSettings":
        self._native_date_in_result_sets = enabled
        return self

    def with_native_datetime_in_result_sets(self, enabled: bool) -> "QueryClientSettings":
        self._native_datetime_in_result_sets = enabled
        return self


class IQuerySessionState(abc.ABC):
    def __init__(self, settings: Optional[QueryClientSettings] = None):
        pass

    @abc.abstractmethod
    def reset(self) -> None:
        pass

    @property
    @abc.abstractmethod
    def session_id(self) -> Optional[str]:
        pass

    @abc.abstractmethod
    def set_session_id(self, session_id: str) -> "IQuerySessionState":
        pass

    @property
    @abc.abstractmethod
    def node_id(self) -> Optional[int]:
        pass

    @abc.abstractmethod
    def set_node_id(self, node_id: int) -> "IQuerySessionState":
        pass

    @property
    @abc.abstractmethod
    def attached(self) -> bool:
        pass

    @abc.abstractmethod
    def set_attached(self, attached: bool) -> "IQuerySessionState":
        pass


def create_execute_query_request(
    query: str,
    session_id: str,
    tx_id: Optional[str],
    commit_tx: Optional[bool],
    tx_mode: Optional[BaseQueryTxMode],
    syntax: Optional[QuerySyntax],
    exec_mode: Optional[QueryExecMode],
    stats_mode: Optional[QueryStatsMode],
    parameters: Optional[dict],
    concurrent_result_sets: Optional[bool],
) -> ydb_query.ExecuteQueryRequest:
    try:
        syntax = QuerySyntax.YQL_V1 if not syntax else syntax
        exec_mode = QueryExecMode.EXECUTE if not exec_mode else exec_mode
        stats_mode = QueryStatsMode.NONE if stats_mode is None else stats_mode

        tx_control = None
        if not tx_id and not tx_mode:
            tx_control = None
        elif tx_id:
            tx_control = ydb_query.TransactionControl(
                tx_id=tx_id,
                commit_tx=commit_tx,
                begin_tx=None,
            )
        else:
            tx_control = ydb_query.TransactionControl(
                begin_tx=ydb_query.TransactionSettings(
                    tx_mode=tx_mode,
                ),
                commit_tx=commit_tx,
                tx_id=None,
            )

        return ydb_query.ExecuteQueryRequest(
            session_id=session_id,
            query_content=ydb_query.QueryContent.from_public(
                query=query,
                syntax=syntax,
            ),
            tx_control=tx_control,
            exec_mode=exec_mode,
            parameters=parameters,
            concurrent_result_sets=concurrent_result_sets,
            stats_mode=stats_mode,
        )
    except BaseException as e:
        raise issues.ClientInternalError("Unable to prepare execute request") from e


def bad_session_handler(func):
    @functools.wraps(func)
    def decorator(rpc_state, response_pb, session_state: IQuerySessionState, *args, **kwargs):
        try:
            return func(rpc_state, response_pb, session_state, *args, **kwargs)
        except issues.BadSession:
            session_state.reset()
            raise

    return decorator


@bad_session_handler
def wrap_execute_query_response(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.ExecuteQueryResponsePart,
    session_state: IQuerySessionState,
    tx: Optional["BaseQueryTxContext"] = None,
    session: Optional["BaseQuerySession"] = None,
    commit_tx: Optional[bool] = False,
    settings: Optional[QueryClientSettings] = None,
) -> convert.ResultSet:
    issues._process_response(response_pb)
    if tx and commit_tx:
        tx._move_to_commited()
    elif tx and response_pb.tx_meta and not tx.tx_id:
        tx._move_to_beginned(response_pb.tx_meta.id)

    if response_pb.HasField("exec_stats"):
        if tx is not None:
            tx._last_query_stats = response_pb.exec_stats
        if session is not None:
            session._last_query_stats = response_pb.exec_stats

    if response_pb.HasField("result_set"):
        return convert.ResultSet.from_message(
            response_pb.result_set,
            settings,
            index=response_pb.result_set_index,
        )

    return None


class TxEvent(enum.Enum):
    BEFORE_COMMIT = "BEFORE_COMMIT"
    AFTER_COMMIT = "AFTER_COMMIT"
    BEFORE_ROLLBACK = "BEFORE_ROLLBACK"
    AFTER_ROLLBACK = "AFTER_ROLLBACK"


class CallbackHandlerMode(enum.Enum):
    SYNC = "SYNC"
    ASYNC = "ASYNC"


def _get_sync_callback(method: typing.Callable, loop: Optional[asyncio.AbstractEventLoop]):
    if asyncio.iscoroutinefunction(method):
        if loop is None:
            loop = _get_shared_event_loop()

        def async_to_sync_callback(*args, **kwargs):
            caller = CallFromSyncToAsync(loop)
            return caller.safe_call_with_result(method(*args, **kwargs), 10)

        return async_to_sync_callback
    return method


def _get_async_callback(method: typing.Callable):
    if asyncio.iscoroutinefunction(method):
        return method

    async def sync_to_async_callback(*args, **kwargs):
        return await to_thread(method, *args, **kwargs, executor=None)

    return sync_to_async_callback


class CallbackHandler:
    def _init_callback_handler(self, mode: CallbackHandlerMode) -> None:
        self._callbacks = defaultdict(list)
        self._callback_mode = mode

    def _execute_callbacks_sync(self, event_name: str, *args, **kwargs) -> None:
        for callback in self._callbacks[event_name]:
            callback(self, *args, **kwargs)

    async def _execute_callbacks_async(self, event_name: str, *args, **kwargs) -> None:
        tasks = [asyncio.create_task(callback(self, *args, **kwargs)) for callback in self._callbacks[event_name]]
        if not tasks:
            return
        await asyncio.gather(*tasks)

    def _prepare_callback(
        self, callback: typing.Callable, loop: Optional[asyncio.AbstractEventLoop]
    ) -> typing.Callable:
        if self._callback_mode == CallbackHandlerMode.SYNC:
            return _get_sync_callback(callback, loop)
        return _get_async_callback(callback)

    def _add_callback(self, event_name: str, callback: typing.Callable, loop: Optional[asyncio.AbstractEventLoop]):
        self._callbacks[event_name].append(self._prepare_callback(callback, loop))
