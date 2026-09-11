LIBRARY()

SRCS(
    runtime.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/executor
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/library/time
)

END()
