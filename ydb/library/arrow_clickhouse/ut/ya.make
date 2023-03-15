UNITTEST_FOR(ydb/library/arrow_clickhouse)

FORK_SUBTESTS()

SPLIT_FACTOR(60)
TIMEOUT(600)
SIZE(MEDIUM)

SRCS(
    ut_aggregator.cpp
)

END()
