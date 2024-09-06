LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_dummy.cpp
    ydb_export.cpp
    ydb_import.cpp
    ydb_logstore.cpp
    ydb_operation.cpp
    ydb_query.cpp
    ydb_scheme.cpp
    ydb_scripting.cpp
    ydb_table.cpp
    ydb_object_storage.cpp
)

PEERDIR(
    library/cpp/monlib/encode
    library/cpp/uri
    ydb/core/base
    ydb/core/client
    ydb/core/formats
    ydb/core/grpc_services
    ydb/core/grpc_services/base
    ydb/core/security
    ydb/core/grpc_streaming
    ydb/core/protos
    ydb/core/scheme
    ydb/library/aclib
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/library/yql/public/types
    contrib/libs/openssl
)

END()

RECURSE_FOR_TESTS(
    backup_ut
    sdk_sessions_ut
    sdk_sessions_pool_ut
    table_split_ut
    ut
)
