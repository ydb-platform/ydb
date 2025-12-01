UNITTEST_FOR(ydb/services/test_shard)

SIZE(MEDIUM)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/protos
    ydb/core/testlib/default
    ydb/services/test_shard
    ydb/core/test_tablet
)

TIMEOUT(60)

YQL_LAST_ABI_VERSION()

END()
