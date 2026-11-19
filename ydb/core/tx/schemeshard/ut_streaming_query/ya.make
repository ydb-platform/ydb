UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/protos/schemeshard
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_operation_tracking.cpp
    ut_streaming_query.cpp
)

END()
