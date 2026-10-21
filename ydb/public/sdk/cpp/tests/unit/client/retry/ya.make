UNITTEST()

SRCS(
    retry_ut.cpp
    retry_async_ut.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/client/impl/internal/retry
    ydb/public/sdk/cpp/src/client/impl/observability
    ydb/public/sdk/cpp/src/client/types
    ydb/public/sdk/cpp/src/client/types/status
    ydb/public/sdk/cpp/src/library/time
)

END()
