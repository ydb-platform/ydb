UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/base
    ydb/core/kqp/ut/common
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg_dummy
)

SRCS(
    ut_consistent_copy_tables.cpp
)

YQL_LAST_ABI_VERSION()

END()
