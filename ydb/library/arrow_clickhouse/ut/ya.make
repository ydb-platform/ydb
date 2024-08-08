UNITTEST_FOR(ydb/library/arrow_clickhouse)

FORK_SUBTESTS()

SPLIT_FACTOR(60)
TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

SRCS(
    ut_aggregator.cpp
)

END()
