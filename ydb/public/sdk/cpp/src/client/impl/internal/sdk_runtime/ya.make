LIBRARY()

SRCS(
    runtime.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/types/exceptions
    ydb/public/sdk/cpp/src/client/types/executor
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()
