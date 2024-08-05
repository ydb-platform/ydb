LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp_v2/headers.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/public/sdk/cpp_v2/src/library/grpc/client
    library/cpp/monlib/metrics
)

END()
