LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    driver.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/common
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/grpc_connections
    ydb/public/sdk/cpp_v2/src/client/resources
    ydb/public/sdk/cpp_v2/src/client/common_client
    ydb/public/sdk/cpp_v2/src/client/types/status
)

END()

RECURSE_FOR_TESTS(
    ut
)
