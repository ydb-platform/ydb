PY3TEST()

TEST_SRCS(
    test_mirror3dc.py
)

PY_SRCS(
    kv_workload.py
    write2x2.py
)

DEPENDS(
    ydb/apps/dstool
    ydb/tools/blobsan
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(ram:16 cpu:1)
TIMEOUT(600)

PEERDIR(
    library/python/testing/yatest_common
    ydb/tests/library
    ydb/tests/library/clients
)

END()
