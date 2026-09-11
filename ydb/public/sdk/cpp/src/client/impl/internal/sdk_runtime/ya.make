LIBRARY()

SRCS(
    runtime.cpp
)

PEERDIR(
    library/cpp/threading/future
    ydb/public/sdk/cpp/src/library/runtime
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/client/types/executor
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
