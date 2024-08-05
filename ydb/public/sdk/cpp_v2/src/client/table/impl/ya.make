LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    client_session.cpp
    data_query.cpp
    readers.cpp
    request_migrator.cpp
    table_client.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/api/protos
    ydb/public/sdk/cpp_v2/src/library/operation_id/protos
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_endpoints
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/session_pool
    ydb/public/sdk/cpp_v2/src/client/table/query_stats
    ydb/public/sdk/cpp_v2/src/library/yql/public/issue/protos
)

END()
