LIBRARY()

SRCS(
    executor.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/ydb_internal/thread_pool
    ydb/public/sdk/cpp/src/client/types/exceptions
)

END()
