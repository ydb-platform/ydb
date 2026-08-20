UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(10)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_all_flags_ut.cpp
)

PEERDIR(
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/public/sdk/cpp/adapters/issue
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
