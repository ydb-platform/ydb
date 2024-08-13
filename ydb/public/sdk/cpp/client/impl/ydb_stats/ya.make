LIBRARY()

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
