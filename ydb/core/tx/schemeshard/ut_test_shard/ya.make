UNITTEST_FOR(ydb/core/tx/schemeshard)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/protos/schemeshard
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/core/test_tablet
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_test_shard.cpp
)

END()
