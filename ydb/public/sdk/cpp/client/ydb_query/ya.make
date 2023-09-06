LIBRARY()

SRCS(
    client.cpp
    client.h
    query.cpp
    query.h
    stats.cpp
    stats.h
    tx.cpp
    tx.h
)

PEERDIR(
    ydb/public/sdk/cpp/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/client/impl/ydb_internal/kqp_session_common
    ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool
    ydb/public/sdk/cpp/client/impl/ydb_internal/retry
    ydb/public/sdk/cpp/client/ydb_common_client
    ydb/public/sdk/cpp/client/ydb_driver
    ydb/public/sdk/cpp/client/ydb_query/impl
    ydb/public/sdk/cpp/client/ydb_result
    ydb/public/sdk/cpp/client/ydb_types/operation
)

END()
