LIBRARY()

SRCS(
    test_shard.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/client/impl/internal/make_request
    ydb/public/sdk/cpp/src/client/common_client/impl
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/api/grpc
)

END()
