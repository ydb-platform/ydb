LIBRARY()

INCLUDE(${ARCADIA_ROOT}/ydb/public/sdk/cpp/client/forbid_peerdir.inc)

SRCS(
    endpoints.cpp
)

PEERDIR(
    library/cpp/monlib/metrics
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/impl/ydb_stats
)

END()

RECURSE_FOR_TESTS(
    ut
)
