LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/sdk_common.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
