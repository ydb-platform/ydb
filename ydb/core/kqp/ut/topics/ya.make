UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

SRCS(
    kqp_topics_ut.cpp
)

PEERDIR(
    ydb/core/kqp/ut/common
    ydb/core/kqp/topics
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
