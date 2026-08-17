UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/protos/schemeshard
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_streaming_query.cpp
)

END()
