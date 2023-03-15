LIBRARY()

SRCS(
    rate_limiter.cpp
)

PEERDIR(
    ydb/public/api/grpc
    ydb/public/sdk/cpp/client/ydb_common_client/impl
    ydb/public/sdk/cpp/client/ydb_driver
)

END()
