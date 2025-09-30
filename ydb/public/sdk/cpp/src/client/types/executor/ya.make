LIBRARY()

SRCS(
    executor.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/executor
    ydb/public/sdk/cpp/src/client/impl/internal/thread_pool
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
