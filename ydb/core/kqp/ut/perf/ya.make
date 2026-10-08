UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

REQUIREMENTS(cpu:2)
SIZE(MEDIUM)

SRCS(
    kqp_query_perf_ut.cpp
    kqp_workload_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/library/workload/kv
    ydb/library/workload/stock
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
