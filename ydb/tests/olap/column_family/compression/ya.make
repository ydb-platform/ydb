PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()

TEST_SRCS(
    base.py
    alter_compression.py
)

SIZE(MEDIUM)

PEERDIR(
    ydb/tests/library
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    ydb/tests/olap/scenario/helpers
    ydb/tests/olap/common
)

DEPENDS(
)

END()

