RECURSE_FOR_TESTS(
    ut
)

LIBRARY()

SRCS(
    mvp.cpp
    mvp.h
    ydbc.cpp
    ydbc_acl.h
    ydbc_backup.h
    ydbc_backup_list.h
    ydbc_config.h
    ydbc_database.h
    ydbc_databases.h
    ydbc_all_databases.h
    ydbc_datastream.h
    ydbc_datastreams.h
    ydbc_list_datastream_consumers.h
    ydbc_directory.h
    ydbc_dynamo_describe_table.h
    ydbc_dynamo_item.h
    ydbc_dynamo_table.h
    ydbc_locations.h
    ydbc_meta.h
    ydbc_operation.h
    ydbc_operations.h
    ydbc_query_helper.h
    ydbc_query.h
    ydbc_query1.h
    ydbc_quota_batch_update_metric.h
    ydbc_quota_get.h
    ydbc_quota_get_default.h
    ydbc_quota_update_metric.h
    ydbc_restore.h
    ydbc_simulate_database.h
    ydbc_start.h
    ydbc_stop.h
    ydbc_table.h
    ydbc_datastreams.h
    yql.cpp
    yql.h
)

PEERDIR(
    ydb/mvp/core
    ydb/mvp/ydbc/protos
    ydb/library/aclib/protos
    ydb/library/yql/public/udf
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/providers/result/expr_nodes
    ydb/library/yql/core/expr_nodes
    ydb/library/yql/sql/pg_dummy
    ydb/library/security
    ydb/core/ydb_convert
    ydb/core/persqueue
    ydb/public/lib/json_value
    library/cpp/protobuf/json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(
    bin
)
