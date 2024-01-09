from . import issues, _session_impl, _apis, types, convert
import functools


def reset_tx_id_handler(func):
    @functools.wraps(func)
    def decorator(rpc_state, response_pb, session_state, tx_state, *args, **kwargs):
        try:
            return func(
                rpc_state, response_pb, session_state, tx_state, *args, **kwargs
            )
        except issues.Error:
            tx_state.tx_id = None
            tx_state.dead = True
            raise

    return decorator


def not_found_handler(func):
    @functools.wraps(func)
    def decorator(
        rpc_state, response_pb, session_state, tx_state, query, *args, **kwargs
    ):
        try:
            return func(
                rpc_state, response_pb, session_state, tx_state, query, *args, **kwargs
            )
        except issues.NotFound:
            session_state.erase(query)
            raise

    return decorator


def wrap_tx_factory_handler(func):
    @functools.wraps(func)
    def decorator(session_state, tx_state, *args, **kwargs):
        if tx_state.dead:
            raise issues.PreconditionFailed(
                "Failed to perform action on broken transaction context!"
            )
        return func(session_state, tx_state, *args, **kwargs)

    return decorator


@_session_impl.bad_session_handler
@reset_tx_id_handler
def wrap_result_on_rollback_or_commit_tx(
    rpc_state, response_pb, session_state, tx_state, tx
):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    # transaction successfully committed or rolled back
    tx_state.tx_id = None
    return tx


@_session_impl.bad_session_handler
def wrap_tx_begin_response(rpc_state, response_pb, session_state, tx_state, tx):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.BeginTransactionResult()
    response_pb.operation.result.Unpack(message)
    tx_state.tx_id = message.tx_meta.id
    return tx


@wrap_tx_factory_handler
def begin_request_factory(session_state, tx_state):
    request = _apis.ydb_table.BeginTransactionRequest()
    request = session_state.start_query().attach_request(request)
    request.tx_settings.MergeFrom(_construct_tx_settings(tx_state))
    return request


@wrap_tx_factory_handler
def rollback_request_factory(session_state, tx_state):
    request = _apis.ydb_table.RollbackTransactionRequest()
    request.tx_id = tx_state.tx_id
    request = session_state.start_query().attach_request(request)
    return request


@wrap_tx_factory_handler
def commit_request_factory(session_state, tx_state):
    """
    Constructs commit request
    """
    request = _apis.ydb_table.CommitTransactionRequest()
    request.tx_id = tx_state.tx_id
    request = session_state.start_query().attach_request(request)
    return request


class TxState(object):
    __slots__ = ("tx_id", "tx_mode", "dead", "initialized")

    def __init__(self, tx_mode):
        """
        Holds transaction context manager info
        :param tx_mode: A mode of transaction
        """
        self.tx_id = None
        self.tx_mode = tx_mode
        self.dead = False
        self.initialized = False


def _construct_tx_settings(tx_state):
    tx_settings = _apis.ydb_table.TransactionSettings()
    mode_property = getattr(tx_settings, tx_state.tx_mode.name)
    mode_property.MergeFrom(tx_state.tx_mode.settings)
    return tx_settings


@wrap_tx_factory_handler
def execute_request_factory(
    session_state, tx_state, query, parameters, commit_tx, settings
):
    data_query, query_id = session_state.lookup(query)
    parameters_types = {}

    if query_id is not None:
        query_pb = _apis.ydb_table.Query(id=query_id)
        parameters_types = data_query.parameters_types
    else:
        if data_query is not None:
            # client cache disabled for send query text every time
            yql_text = data_query.yql_text
            parameters_types = data_query.parameters_types
        elif isinstance(query, types.DataQuery):
            yql_text = query.yql_text
            parameters_types = query.parameters_types
        else:
            yql_text = query
        query_pb = _apis.ydb_table.Query(yql_text=yql_text)
    request = _apis.ydb_table.ExecuteDataQueryRequest(
        parameters=convert.parameters_to_pb(parameters_types, parameters)
    )

    if query_id is not None:
        # SDK not send query text and nothing save to cache
        keep_in_cache = False
    elif settings is not None and hasattr(settings, "keep_in_cache"):
        keep_in_cache = settings.keep_in_cache
    elif parameters:
        keep_in_cache = True
    else:
        keep_in_cache = False

    if keep_in_cache:
        request.query_cache_policy.keep_in_cache = True

    request.query.MergeFrom(query_pb)
    tx_control = _apis.ydb_table.TransactionControl()
    tx_control.commit_tx = commit_tx
    if tx_state.tx_id is not None:
        tx_control.tx_id = tx_state.tx_id
    else:
        tx_control.begin_tx.MergeFrom(_construct_tx_settings(tx_state))
    request.tx_control.MergeFrom(tx_control)
    request = session_state.start_query().attach_request(request)
    return request


@_session_impl.bad_session_handler
@reset_tx_id_handler
@not_found_handler
def wrap_result_and_tx_id(rpc_state, response_pb, session_state, tx_state, query):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.ExecuteQueryResult()
    response_pb.operation.result.Unpack(message)
    if message.query_meta.id:
        session_state.keep(query, message.query_meta.id)
    tx_state.tx_id = None if not message.tx_meta.id else message.tx_meta.id
    return convert.ResultSets(message.result_sets, session_state.table_client_settings)
