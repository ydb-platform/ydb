UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(4)
REQUIREMENTS(cpu:4)

SIZE(MEDIUM)

SRCS(
    kqp_pattern_cache_ut.cpp
    kqp_qs_queries_heavy_ut.cpp
)

PEERDIR(
    contrib/libs/fmt
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/tx/columnshard/hooks/testing
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    ydb/public/lib/ut_helpers
    ydb/public/sdk/cpp/src/client/operation
    ydb/public/sdk/cpp/src/client/types/operation
)

YQL_LAST_ABI_VERSION()

END()
