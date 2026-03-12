PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")

PY_SRCS (
    common.py
    helpers.py
)
TEST_SRCS(
    test_nbs.py
    test_nbs_load_actor.py
)

SIZE(MEDIUM)

DEPENDS(
    ydb/apps/dstool
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
)

END()
