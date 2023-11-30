LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/monlib/metrics
)

END()
