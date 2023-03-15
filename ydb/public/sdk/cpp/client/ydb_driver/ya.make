LIBRARY()

SRCS(
    driver.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/impl/ydb_internal/common
    ydb/public/sdk/cpp/client/impl/ydb_internal/grpc_connections
    ydb/public/sdk/cpp/client/resources
    ydb/public/sdk/cpp/client/ydb_common_client
    ydb/public/sdk/cpp/client/ydb_types/status
)

END()

RECURSE_FOR_TESTS(
    ut
)
