LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    ydb/public/sdk/cpp/src/client/metrics
    library/cpp/monlib/metrics
)

END()
