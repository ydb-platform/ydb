UNITTEST_FOR(ydb/core/tx/schemeshard)

FORK_SUBTESTS()

TIMEOUT(600)

SIZE(MEDIUM)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/tx/schemeshard/ut_helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

SRCS(
    ut_consistent_copy_tables.cpp
)

YQL_LAST_ABI_VERSION()

END()
