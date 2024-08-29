import abc
import enum
import functools

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

if typing.TYPE_CHECKING:
    from .transaction import BaseQueryTxContext


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


class StatsMode(enum.IntEnum):
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
    parameters: Optional[dict],
    concurrent_result_sets: Optional[bool],
) -> ydb_query.ExecuteQueryRequest:
    syntax = QuerySyntax.YQL_V1 if not syntax else syntax
    exec_mode = QueryExecMode.EXECUTE if not exec_mode else exec_mode
    stats_mode = StatsMode.NONE  # TODO: choise is not supported yet

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


def wrap_execute_query_response(
    rpc_state: RpcState,
    response_pb: _apis.ydb_query.ExecuteQueryResponsePart,
    tx: Optional["BaseQueryTxContext"] = None,
    commit_tx: Optional[bool] = False,
    settings: Optional[QueryClientSettings] = None,
) -> convert.ResultSet:
    issues._process_response(response_pb)
    if tx and response_pb.tx_meta and not tx.tx_id:
        tx._move_to_beginned(response_pb.tx_meta.id)
    if tx and commit_tx:
        tx._move_to_commited()
    return convert.ResultSet.from_message(response_pb.result_set, settings)


def bad_session_handler(func):
    @functools.wraps(func)
    def decorator(rpc_state, response_pb, session_state: IQuerySessionState, *args, **kwargs):
        try:
            return func(rpc_state, response_pb, session_state, *args, **kwargs)
        except issues.BadSession:
            session_state.reset()
            raise

    return decorator
