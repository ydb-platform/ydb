LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    authenticator.cpp
    endpoint_pool.cpp
    state.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/threading/future
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_endpoints
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/logger
    ydb/public/sdk/cpp_v2/src/client/impl/ydb_internal/plain_status
    ydb/public/sdk/cpp_v2/src/client/types/credentials
)

END()
