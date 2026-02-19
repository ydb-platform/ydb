UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(SMALL)

SRCS(
    kqp_tli_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/library/actors/wilson/test_util
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
