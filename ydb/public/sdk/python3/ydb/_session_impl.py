import functools
from google.protobuf.empty_pb2 import Empty
from . import issues, types, _apis, convert, scheme, operation, _utilities

X_YDB_SERVER_HINTS = "x-ydb-server-hints"
X_YDB_SESSION_CLOSE = "session-close"


def _check_session_is_closing(rpc_state, session_state):
    metadata = rpc_state.trailing_metadata()
    if X_YDB_SESSION_CLOSE in metadata.get(X_YDB_SERVER_HINTS, []):
        session_state.set_closing()


def bad_session_handler(func):
    @functools.wraps(func)
    def decorator(rpc_state, response_pb, session_state, *args, **kwargs):
        try:
            _check_session_is_closing(rpc_state, session_state)
            return func(rpc_state, response_pb, session_state, *args, **kwargs)
        except issues.BadSession:
            session_state.reset()
            raise

    return decorator


@bad_session_handler
def wrap_prepare_query_response(rpc_state, response_pb, session_state, yql_text):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.PrepareQueryResult()
    response_pb.operation.result.Unpack(message)
    data_query = types.DataQuery(yql_text, message.parameters_types)
    session_state.keep(data_query, message.query_id)
    return data_query


def prepare_request_factory(session_state, yql_text):
    request = session_state.start_query().attach_request(_apis.ydb_table.PrepareDataQueryRequest())
    request.yql_text = yql_text
    return request


class AlterTableOperation(operation.Operation):
    def __init__(self, rpc_state, response_pb, driver):
        super(AlterTableOperation, self).__init__(rpc_state, response_pb, driver)
        self.ready = response_pb.operation.ready


def copy_tables_request_factory(session_state, source_destination_pairs):
    request = session_state.attach_request(_apis.ydb_table.CopyTablesRequest())
    for source_path, destination_path in source_destination_pairs:
        table_item = request.tables.add()
        table_item.source_path = source_path
        table_item.destination_path = destination_path
    return request


def rename_tables_request_factory(session_state, rename_items):
    request = session_state.attach_request(_apis.ydb_table.RenameTablesRequest())
    for item in rename_items:
        table_item = request.tables.add()
        table_item.source_path = item.source_path
        table_item.destination_path = item.destination_path
        table_item.replace_destination = item.replace_destination
    return request


def explain_data_query_request_factory(session_state, yql_text):
    request = session_state.start_query().attach_request(_apis.ydb_table.ExplainDataQueryRequest())
    request.yql_text = yql_text
    return request


class _ExplainResponse(object):
    def __init__(self, ast, plan):
        self.query_ast = ast
        self.query_plan = plan


def wrap_explain_response(rpc_state, response_pb, session_state):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.ExplainQueryResult()
    response_pb.operation.result.Unpack(message)
    return _ExplainResponse(message.query_ast, message.query_plan)


@bad_session_handler
def wrap_execute_scheme_result(rpc_state, response_pb, session_state):
    session_state.complete_query()
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.ExecuteQueryResult()
    response_pb.operation.result.Unpack(message)
    return convert.ResultSets(message.result_sets)


def execute_scheme_request_factory(session_state, yql_text):
    request = session_state.start_query().attach_request(_apis.ydb_table.ExecuteSchemeQueryRequest())
    request.yql_text = yql_text
    return request


@bad_session_handler
def wrap_describe_table_response(rpc_state, response_pb, sesssion_state, scheme_entry_cls):
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.DescribeTableResult()
    response_pb.operation.result.Unpack(message)
    return scheme._wrap_scheme_entry(
        message.self,
        scheme_entry_cls,
        message.columns,
        message.primary_key,
        message.shard_key_bounds,
        message.indexes,
        message.table_stats if message.HasField("table_stats") else None,
        message.ttl_settings if message.HasField("ttl_settings") else None,
        message.attributes,
        message.partitioning_settings if message.HasField("partitioning_settings") else None,
        message.column_families,
        message.key_bloom_filter,
        message.read_replicas_settings if message.HasField("read_replicas_settings") else None,
        message.storage_settings if message.HasField("storage_settings") else None,
    )


def explicit_partitions_factory(primary_key, columns, split_points):
    column_types = {}
    pk = set(primary_key)
    for column in columns:
        if column.name in pk:
            column_types[column.name] = column.type

    explicit_partitions = _apis.ydb_table.ExplicitPartitions()
    for split_point in split_points:
        typed_value = explicit_partitions.split_points.add()
        split_point_type = types.TupleType()
        prefix_size = len(split_point.value)
        for pl_el_id, pk_name in enumerate(primary_key):
            if pl_el_id >= prefix_size:
                break

            split_point_type.add_element(column_types[pk_name])

        typed_value.type.MergeFrom(split_point_type.proto)
        typed_value.value.MergeFrom(convert.from_native_value(split_point_type.proto, split_point.value))

    return explicit_partitions


def create_table_request_factory(session_state, path, table_description):
    if isinstance(table_description, _apis.ydb_table.CreateTableRequest):
        request = session_state.attach_request(table_description)
        return request

    request = _apis.ydb_table.CreateTableRequest()
    request.path = path
    request.primary_key.extend(list(table_description.primary_key))
    for column in table_description.columns:
        request.columns.add(name=column.name, type=column.type_pb, family=column.family)

    if table_description.profile is not None:
        request.profile.MergeFrom(table_description.profile.to_pb(table_description))

    for index in table_description.indexes:
        request.indexes.add().MergeFrom(index.to_pb())

    if table_description.ttl_settings is not None:
        request.ttl_settings.MergeFrom(table_description.ttl_settings.to_pb())

    request.attributes.update(table_description.attributes)

    if table_description.column_families:
        for column_family in table_description.column_families:
            request.column_families.add().MergeFrom(column_family.to_pb())

    if table_description.storage_settings is not None:
        request.storage_settings.MergeFrom(table_description.storage_settings.to_pb())

    if table_description.read_replicas_settings is not None:
        request.read_replicas_settings.MergeFrom(table_description.read_replicas_settings.to_pb())

    if table_description.partitioning_settings is not None:
        request.partitioning_settings.MergeFrom(table_description.partitioning_settings.to_pb())

    request.key_bloom_filter = table_description.key_bloom_filter
    if table_description.compaction_policy is not None:
        request.compaction_policy = table_description.compaction_policy
    if table_description.partition_at_keys is not None:
        request.partition_at_keys.MergeFrom(
            explicit_partitions_factory(
                list(table_description.primary_key),
                table_description.columns,
                table_description.partition_at_keys.split_points,
            )
        )

    elif table_description.uniform_partitions > 0:
        request.uniform_partitions = table_description.uniform_partitions

    return session_state.attach_request(request)


def keep_alive_request_factory(session_state):
    request = _apis.ydb_table.KeepAliveRequest()
    return session_state.attach_request(request)


@bad_session_handler
def cleanup_session(rpc_state, response_pb, session_state, session):
    issues._process_response(response_pb.operation)
    session_state.reset()
    return session


@bad_session_handler
def initialize_session(rpc_state, response_pb, session_state, session):
    issues._process_response(response_pb.operation)
    message = _apis.ydb_table.CreateSessionResult()
    response_pb.operation.result.Unpack(message)
    session_state.set_id(message.session_id).attach_endpoint(rpc_state.endpoint_key)
    return session


@bad_session_handler
def wrap_operation(rpc_state, response_pb, session_state, driver=None):
    return operation.Operation(rpc_state, response_pb, driver)


def wrap_operation_bulk_upsert(rpc_state, response_pb, driver=None):
    return operation.Operation(rpc_state, response_pb, driver)


@bad_session_handler
def wrap_keep_alive_response(rpc_state, response_pb, session_state, session):
    issues._process_response(response_pb.operation)
    return session


def describe_table_request_factory(session_state, path, settings=None):
    request = session_state.attach_request(_apis.ydb_table.DescribeTableRequest())
    request.path = path

    if settings is not None and hasattr(settings, "include_shard_key_bounds") and settings.include_shard_key_bounds:
        request.include_shard_key_bounds = settings.include_shard_key_bounds

    if settings is not None and hasattr(settings, "include_table_stats") and settings.include_table_stats:
        request.include_table_stats = settings.include_table_stats

    return request


def alter_table_request_factory(
    session_state,
    path,
    add_columns,
    drop_columns,
    alter_attributes,
    add_indexes,
    drop_indexes,
    set_ttl_settings,
    drop_ttl_settings,
    add_column_families,
    alter_column_families,
    alter_storage_settings,
    set_compaction_policy,
    alter_partitioning_settings,
    set_key_bloom_filter,
    set_read_replicas_settings,
):
    request = session_state.attach_request(_apis.ydb_table.AlterTableRequest(path=path))
    if add_columns is not None:
        for column in add_columns:
            request.add_columns.add(name=column.name, type=column.type_pb)

    if drop_columns is not None:
        request.drop_columns.extend(list(drop_columns))

    if drop_indexes is not None:
        request.drop_indexes.extend(list(drop_indexes))

    if add_indexes is not None:
        for index in add_indexes:
            request.add_indexes.add().MergeFrom(index.to_pb())

    if alter_attributes is not None:
        request.alter_attributes.update(alter_attributes)

    if set_ttl_settings is not None:
        request.set_ttl_settings.MergeFrom(set_ttl_settings.to_pb())

    if drop_ttl_settings is not None and drop_ttl_settings:
        request.drop_ttl_settings.MergeFrom(Empty())

    if add_column_families is not None:
        for column_family in add_column_families:
            request.add_column_families.add().MergeFrom(column_family.to_pb())

    if alter_column_families is not None:
        for column_family in alter_column_families:
            request.alter_column_families.add().MergeFrom(column_family.to_pb())

    if alter_storage_settings is not None:
        request.alter_storage_settings.MergeFrom(alter_storage_settings.to_pb())

    if set_compaction_policy is not None:
        request.set_compaction_policy = set_compaction_policy

    if alter_partitioning_settings is not None:
        request.alter_partitioning_settings.MergeFrom(alter_partitioning_settings.to_pb())

    if set_key_bloom_filter is not None:
        request.set_key_bloom_filter = set_key_bloom_filter

    if set_read_replicas_settings is not None:
        request.set_read_replicas_settings.MergeFrom(set_read_replicas_settings.to_pb())

    return request


def read_table_request_factory(
    session_state,
    path,
    key_range=None,
    columns=None,
    ordered=False,
    row_limit=None,
    use_snapshot=None,
):
    request = _apis.ydb_table.ReadTableRequest()
    request.path = path
    request.ordered = ordered
    if key_range is not None and key_range.from_bound is not None:
        target_attribute = "greater_or_equal" if key_range.from_bound.is_inclusive() else "greater"
        getattr(request.key_range, target_attribute).MergeFrom(
            convert.to_typed_value_from_native(key_range.from_bound.type, key_range.from_bound.value)
        )

    if key_range is not None and key_range.to_bound is not None:
        target_attribute = "less_or_equal" if key_range.to_bound.is_inclusive() else "less"
        getattr(request.key_range, target_attribute).MergeFrom(
            convert.to_typed_value_from_native(key_range.to_bound.type, key_range.to_bound.value)
        )

    if columns is not None:
        for column in columns:
            request.columns.append(column)
    if row_limit:
        # NOTE(gvit): pylint cannot understand that row_limit is not None
        request.row_limit = row_limit  # pylint: disable=E5903
    if use_snapshot is not None:
        if isinstance(use_snapshot, bool):
            if use_snapshot:
                request.use_snapshot = _apis.FeatureFlag.ENABLED
            else:
                request.use_snapshot = _apis.FeatureFlag.DISABLED
        else:
            request.use_snapshot = use_snapshot
    return session_state.attach_request(request)


def bulk_upsert_request_factory(table, rows, column_types):
    request = _apis.ydb_table.BulkUpsertRequest()
    request.table = table
    request.rows.MergeFrom(convert.to_typed_value_from_native(types.ListType(column_types).proto, rows))
    return request


def wrap_read_table_response(response):
    issues._process_response(response)
    snapshot = response.snapshot if response.HasField("snapshot") else None
    return convert.ResultSet.from_message(response.result.result_set, snapshot=snapshot)


class SessionState(object):
    def __init__(self, table_client_settings):
        self._session_id = None
        self._query_cache = _utilities.LRUCache(1000)
        self._default = (None, None)
        self._pending_query = False
        self._endpoint = None
        self._closing = False
        self._client_cache_enabled = table_client_settings._client_query_cache_enabled
        self.table_client_settings = table_client_settings

    def __contains__(self, query):
        return self.lookup(query) != self._default

    def reset(self):
        self._query_cache = _utilities.LRUCache(1000)
        self._session_id = None
        self._pending_query = False
        self._endpoint = None

    def attach_endpoint(self, endpoint):
        self._endpoint = endpoint
        return self

    def set_closing(self):
        self._closing = True
        return self

    def closing(self):
        return self._closing

    @property
    def endpoint(self):
        return self._endpoint

    @property
    def session_id(self):
        return self._session_id

    def pending_query(self):
        return self._pending_query

    def set_id(self, session_id):
        self._session_id = session_id
        return self

    def keep(self, query, query_id):
        if self._client_cache_enabled:
            self._query_cache.put(query.name, (query, query_id))
        else:
            self._query_cache.put(query.name, (query, None))
        return self

    @staticmethod
    def _query_key(query):
        return query.name if isinstance(query, types.DataQuery) else _utilities.get_query_hash(query)

    def lookup(self, query):
        return self._query_cache.get(self._query_key(query), self._default)

    def erase(self, query):
        query, _ = self.lookup(query)
        self._query_cache.erase(query.name)

    def complete_query(self):
        self._pending_query = False
        return self

    def start_query(self):
        if self._pending_query:
            # don't invalidate session at this point
            self.reset()
            raise issues.BadSession("Pending previous query completion!")
        self._pending_query = True
        return self

    def attach_request(self, request):
        if self._session_id is None:
            raise issues.BadSession("Empty session_id")
        request.session_id = self._session_id
        return request
