UNITTEST_FOR(ydb/library/yql/providers/generic/pushdown)

SRCS(
    match_predicate_ut.cpp
)

SIZE(SMALL)
REQUIREMENTS(cpu:1)

YQL_LAST_ABI_VERSION()

END()
