LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/client/metrics
    ydb/public/sdk/cpp/src/client/impl/observability/error_category
    library/cpp/monlib/metrics
)

END()
