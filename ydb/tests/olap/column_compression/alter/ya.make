PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

FORK_SUBTESTS()

SIZE(MEDIUM)

TIMEOUT(300)

TEST_SRCS(
    alter_compression.py
)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/olap/scenario/helpers
    ydb/tests/olap/common
    ydb/tests/olap/column_compression/common
)

DEPENDS(
)

END()
