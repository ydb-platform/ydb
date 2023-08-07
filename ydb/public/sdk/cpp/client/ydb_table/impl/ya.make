LIBRARY()

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
    ydb/public/lib/operation_id/protos
    ydb/public/sdk/cpp/client/impl/ydb_endpoints
    ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool
    ydb/public/sdk/cpp/client/ydb_table/query_stats
    ydb/library/yql/public/issue/protos
)

END()
