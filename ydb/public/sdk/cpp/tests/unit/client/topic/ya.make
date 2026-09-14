UNITTEST()

SIZE(SMALL)
FORK_SUBTESTS()

PEERDIR(
    library/cpp/logger
    library/cpp/monlib/dynamic_counters
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/impl/internal/grpc_connections
    ydb/public/sdk/cpp/src/client/topic
    ydb/public/sdk/cpp/src/client/topic/impl
)

SRCS(
    codecs_ut.cpp
    runtime_lifetime_ut.cpp
)

END()
