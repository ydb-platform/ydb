LIBRARY()

SRCS(
    client.cpp
    query.cpp
    stats.cpp
    tx.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/make_request
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/kqp_session_common
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/session_pool
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/retry
    ydb/public/sdk/cpp/src/client/common_client
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query/impl
    ydb/public/sdk/cpp/src/client/result
    ydb/public/sdk/cpp/src/client/types/operation
)

END()
