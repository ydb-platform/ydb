UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

SIZE(MEDIUM)

PEERDIR(
    ydb/core/protos/schemeshard
    ydb/core/testlib/default
    ydb/core/tx/schemeshard/ut_helpers
    ydb/services/metadata/abstract
)

YQL_LAST_ABI_VERSION()

SRCS(
    ut_operation_tracking.cpp
    ut_streaming_query.cpp
)

END()
