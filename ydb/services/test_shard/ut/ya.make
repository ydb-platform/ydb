UNITTEST_FOR(ydb/services/test_shard)

SIZE(MEDIUM)

SRCS(
    grpc_service_ut.cpp
)

PEERDIR(
    library/cpp/logger
    ydb/core/testlib/basics
    ydb/core/testlib/default
    ydb/core/tx/scheme_cache
    ydb/public/api/grpc
    ydb/public/sdk/cpp/src/client/resources
    ydb/public/sdk/cpp/src/library/grpc/client
)

TIMEOUT(60)

YQL_LAST_ABI_VERSION()

END()
