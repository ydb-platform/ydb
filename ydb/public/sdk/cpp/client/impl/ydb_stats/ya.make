LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    stats.cpp
)

PEERDIR(
    ydb/library/grpc/client
    library/cpp/monlib/metrics
)

END()
