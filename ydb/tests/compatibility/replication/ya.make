PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

SIZE(LARGE)
TIMEOUT(1800)
REQUIREMENTS(ram:32 cpu:8)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

TEST_SRCS(test_force_drop.py)

DEPENDS(ydb/tests/library/compatibility/binaries)

PEERDIR(
    contrib/python/grpcio
    contrib/python/ydb/py3
    ydb/public/api/protos
    ydb/tests/library
    ydb/tests/library/compatibility
)

END()
