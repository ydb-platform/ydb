INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

ENV(YDB_DSTOOL_BINARY="ydb/apps/dstool/ydb-dstool")

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(cpu:4)
REQUIREMENTS(ram:16)

DEPENDS(
    ydb/apps/dstool
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/tests/functional/nbs/lib
)
