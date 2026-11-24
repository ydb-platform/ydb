GTEST()

FORK_SUBTESTS()

SRCS(
    operation_ut.cpp
)

PEERDIR(
    contrib/libs/grpc
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/draft
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/export
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/table
)

END()
