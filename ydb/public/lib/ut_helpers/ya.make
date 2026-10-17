LIBRARY()

SRCS(
    ut_helpers_query.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/public/api/grpc
    ydb/public/api/grpc/draft
    ydb/public/api/protos
    ydb/public/api/protos/out
    ydb/public/sdk/cpp/src/library/grpc/client
)

END()

