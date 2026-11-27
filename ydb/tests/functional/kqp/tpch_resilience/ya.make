PY3TEST()

TEST_SRCS(
    test_tpch_resilience.py
)

SIZE(LARGE)
TIMEOUT(1800)
REQUIREMENTS(ram:32 cpu:8)
FORK_SUBTESTS()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
ENV(YDB_ENABLE_COLUMN_TABLES=true)

PEERDIR(
    ydb/public/sdk/python
    ydb/tests/library
)

DATA(
    arcadia/ydb/library/benchmarks/queries/tpch/yql/q15.sql
    arcadia/ydb/library/benchmarks/gen_queries/consts.yql
)

END()
