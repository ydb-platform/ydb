LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
