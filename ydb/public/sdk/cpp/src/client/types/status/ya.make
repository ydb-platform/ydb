LIBRARY()

SRCS(
    status.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/impl/internal/plain_status
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/fatal_error_handlers
    ydb/public/sdk/cpp/src/library/issue
)

END()
