LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/headers.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
