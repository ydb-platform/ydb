LIBRARY()

SRCS(
    ydb_clickhouse_internal.cpp
    ydb_debug.cpp
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
    yql/essentials/public/types
    contrib/libs/openssl
)

END()

RECURSE(
    ut_common
)

RECURSE_FOR_TESTS(
    backup_ut
    table_split_ut
    ut
    ut_database_quotas
    ut_grpc_connstr
    ut_grpc_low
    ut_grpc_sdk
    ut_grpc_ydb
    ut_locality
    ut_table_profile
    ut_yql_types
)
