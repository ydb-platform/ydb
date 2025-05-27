LIBRARY()

SRCS(
    driver.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/common
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/grpc_connections
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/client/common_client
    ydb/public/sdk/cpp/src/client/types/status
)

END()
