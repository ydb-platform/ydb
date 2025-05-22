LIBRARY()

SRCS(
    config.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/table
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/public/sdk/cpp/src/client/value
)

END()
