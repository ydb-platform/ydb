UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(3)

SIZE(MEDIUM)

SRCS(
    kqp_user_facing_trace_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/testlib/default
    ydb/core/tx/datashard/ut_common
    ydb/library/actors/wilson/test_util
)

YQL_LAST_ABI_VERSION()

END()
