LIBRARY()

SRCS(
    endpoints.cpp
)

PEERDIR(
    ydb/public/sdk/cpp/client/impl/ydb_stats
    library/cpp/monlib/metrics
    ydb/public/api/grpc
)

END()

RECURSE_FOR_TESTS(
    ut
)
