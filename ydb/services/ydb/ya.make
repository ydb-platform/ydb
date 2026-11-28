LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_database_management.cpp
    ydb_debug.cpp
    ydb_dummy.cpp
    ydb_export.cpp
    ydb_import.cpp
    ydb_logstore.cpp
    ydb_operation.cpp
    ydb_query.cpp
    ydb_scheme.cpp
    ydb_secret.cpp
    ydb_scripting.cpp
    ydb_table.cpp
    ydb_udf.cpp
    ydb_object_storage.cpp
)

PEERDIR(
    ydb/core/base
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/grpc_services/counters
    ydb/core/grpc_streaming
    ydb/core/protos/schemeshard
    # UDF RPC implementations declared in core/grpc_services/service_udf.h.
    ydb/core/udf_api
    ydb/library/actors/core
    ydb/library/grpc/server
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    # Monitoring settings used by core/persqueue/public/schema.
    ydb/services/lib/actors
)

END()

RECURSE_FOR_TESTS(
    backup_ut
    table_split_ut
    ut
)
