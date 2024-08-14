import abc
import enum
import functools

from typing import (
    Iterator,
    Optional,
)

from .._grpc.grpcwrapper.common_utils import (
    SupportedDriverType,
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
        for _ in self:
            pass


class QueryClientSettings:
    pass


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


class IQuerySession(abc.ABC):
    """Session object for Query Service. It is not recommended to control
    session's lifecycle manually - use a QuerySessionPool is always a better choise.
    """

    @abc.abstractmethod
    def __init__(self, driver: SupportedDriverType, settings: Optional[QueryClientSettings] = None):
        pass

    @abc.abstractmethod
    def create(self) -> "IQuerySession":
        """WARNING: This API is experimental and could be changed.

        Creates a Session of Query Service on server side and attaches it.

        :return: Session object.
        """
        pass

    @abc.abstractmethod
    def delete(self) -> None:
        """WARNING: This API is experimental and could be changed.

        Deletes a Session of Query Service on server side and releases resources.

        :return: None
        """
        pass

    @abc.abstractmethod
    def transaction(self, tx_mode: Optional[BaseQueryTxMode] = None) -> "IQueryTxContext":
        """WARNING: This API is experimental and could be changed.

        Creates a transaction context manager with specified transaction mode.

        :param tx_mode: Transaction mode, which is a one from the following choises:
         1) QuerySerializableReadWrite() which is default mode;
         2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
         3) QuerySnapshotReadOnly();
         4) QueryStaleReadOnly().

        :return: transaction context manager.
        """
        pass

    @abc.abstractmethod
    def execute(
        self,
        query: str,
        syntax: Optional[QuerySyntax] = None,
        exec_mode: Optional[QueryExecMode] = None,
        parameters: Optional[dict] = None,
        concurrent_result_sets: Optional[bool] = False,
    ) -> Iterator:
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


class IQueryTxContext(abc.ABC):
    """
    An object that provides a simple transaction context manager that allows statements execution
    in a transaction. You don't have to open transaction explicitly, because context manager encapsulates
    transaction control logic, and opens new transaction if:
     1) By explicit .begin();
     2) On execution of a first statement, which is strictly recommended method, because that avoids
     useless round trip

    This context manager is not thread-safe, so you should not manipulate on it concurrently.
    """

    @abc.abstractmethod
    def __init__(
        self,
        driver: SupportedDriverType,
        session_state: IQuerySessionState,
        session: IQuerySession,
        tx_mode: BaseQueryTxMode,
    ):
        """
        An object that provides a simple transaction context manager that allows statements execution
        in a transaction. You don't have to open transaction explicitly, because context manager encapsulates
        transaction control logic, and opens new transaction if:

        1) By explicit .begin() method;
        2) On execution of a first statement, which is strictly recommended method, because that avoids useless round trip

        This context manager is not thread-safe, so you should not manipulate on it concurrently.

        :param driver: A driver instance
        :param session_state: A state of session
        :param tx_mode: Transaction mode, which is a one from the following choises:
         1) QuerySerializableReadWrite() which is default mode;
         2) QueryOnlineReadOnly(allow_inconsistent_reads=False);
         3) QuerySnapshotReadOnly();
         4) QueryStaleReadOnly().
        """
        pass

    @abc.abstractmethod
    def __enter__(self) -> "IQueryTxContext":
        """
        Enters a context manager and returns a transaction

        :return: A transaction instance
        """
        pass

    @abc.abstractmethod
    def __exit__(self, *args, **kwargs):
        """
        Closes a transaction context manager and rollbacks transaction if
        it is not finished explicitly
        """
        pass

    @property
    @abc.abstractmethod
    def session_id(self) -> str:
        """
        A transaction's session id

        :return: A transaction's session id
        """
        pass

    @property
    @abc.abstractmethod
    def tx_id(self) -> Optional[str]:
        """
        Returns an id of open transaction or None otherwise

        :return: An id of open transaction or None otherwise
        """
        pass

    @abc.abstractmethod
    def begin(self, settings: Optional[QueryClientSettings] = None) -> None:
        """WARNING: This API is experimental and could be changed.

        Explicitly begins a transaction

        :param settings: A request settings

        :return: None or exception if begin is failed
        """
        pass

    @abc.abstractmethod
    def commit(self, settings: Optional[QueryClientSettings] = None) -> None:
        """WARNING: This API is experimental and could be changed.

        Calls commit on a transaction if it is open. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: None or exception if commit is failed
        """
        pass

    @abc.abstractmethod
    def rollback(self, settings: Optional[QueryClientSettings] = None) -> None:
        """WARNING: This API is experimental and could be changed.

        Calls rollback on a transaction if it is open. If transaction execution
        failed then this method raises PreconditionFailed.

        :param settings: A request settings

        :return: None or exception if rollback is failed
        """
        pass

    @abc.abstractmethod
    def execute(
        self,
        query: str,
        commit_tx: Optional[bool] = False,
        syntax: Optional[QuerySyntax] = None,
        exec_mode: Optional[QueryExecMode] = None,
        parameters: Optional[dict] = None,
        concurrent_result_sets: Optional[bool] = False,
    ) -> Iterator:
        """WARNING: This API is experimental and could be changed.

        Sends a query to Query Service
        :param query: (YQL or SQL text) to be executed.
        :param commit_tx: A special flag that allows transaction commit.
        :param syntax: Syntax of the query, which is a one from the following choises:
         1) QuerySyntax.YQL_V1, which is default;
         2) QuerySyntax.PG.
        :param exec_mode: Exec mode of the query, which is a one from the following choises:
         1) QueryExecMode.EXECUTE, which is default;
         2) QueryExecMode.EXPLAIN;
         3) QueryExecMode.VALIDATE;
         4) QueryExecMode.PARSE.
        :param parameters: dict with parameters and YDB types;
        :param concurrent_result_sets: A flag to allow YDB mix parts of different result sets. Default is False;

        :return: Iterator with result sets
        """
        pass


class IQueryClient(abc.ABC):
    def __init__(self, driver: SupportedDriverType, query_client_settings: Optional[QueryClientSettings] = None):
        pass

    @abc.abstractmethod
    def session(self) -> IQuerySession:
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
    tx: Optional[IQueryTxContext] = None,
    commit_tx: Optional[bool] = False,
) -> convert.ResultSet:
    issues._process_response(response_pb)
    if tx and response_pb.tx_meta and not tx.tx_id:
        tx._move_to_beginned(response_pb.tx_meta.id)
    if tx and commit_tx:
        tx._move_to_commited()
    return convert.ResultSet.from_message(response_pb.result_set)


def bad_session_handler(func):
    @functools.wraps(func)
    def decorator(rpc_state, response_pb, session_state: IQuerySessionState, *args, **kwargs):
        try:
            return func(rpc_state, response_pb, session_state, *args, **kwargs)
        except issues.BadSession:
            session_state.reset()
            raise

    return decorator
