LIBRARY()

SRCS(
    authenticator.cpp
    endpoint_pool.cpp
    state.cpp
)

PEERDIR(
    library/cpp/string_utils/quote
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/impl/endpoints
    ydb/public/sdk/cpp/src/client/impl/internal/logger
    ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    ydb/public/sdk/cpp/src/client/types/credentials
)

END()
