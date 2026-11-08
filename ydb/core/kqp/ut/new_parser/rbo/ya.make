PY3TEST()

TEST_SRCS(test_rbo.py)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
FORK_SUBTESTS()
SPLIT_FACTOR(64)
TIMEOUT(1800)
REQUIREMENTS(cpu:2)

PEERDIR(library/python/testing/yatest_common)

# Reuse the complete binaries, including their embedded resources and original
# source-relative data paths. No copies of query text or expected results.
DEPENDS(
    ydb/core/kqp/ut/rbo
    ydb/core/kqp/ut/rbo/aggregation
    ydb/core/kqp/ut/rbo/basic
    ydb/core/kqp/ut/rbo/explain
    ydb/core/kqp/ut/rbo/fallback
    ydb/core/kqp/ut/rbo/join_order
    ydb/core/kqp/ut/rbo/joins
    ydb/core/kqp/ut/rbo/metadata
    ydb/core/kqp/ut/rbo/olap
    ydb/core/kqp/ut/rbo/physical
    ydb/core/kqp/ut/rbo/reads
    ydb/core/kqp/ut/rbo/subqueries
    ydb/core/kqp/ut/rbo/workloads
    ydb/core/kqp/ut/new_parser/fallback
)

DATA(
    arcadia/ydb/core/kqp/ut/join/data
    arcadia/ydb/core/kqp/ut/rbo/data
)

END()
