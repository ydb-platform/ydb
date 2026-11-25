PY3TEST()

TEST_SRCS(
    test_mirror3dc.py
    test_mirror3dc_chaos.py
)

PY_SRCS(
    kv_churn.py
    kv_workload.py
    mirror3dc_base.py
    transition_barrier.py
    transition_cycle.py
    transition_layout.py
    write2x2.py
)

DEPENDS(
    ydb/apps/dstool
    ydb/tools/blobsan
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
REQUIREMENTS(ram:16 cpu:9)
TIMEOUT(600)
FORK_SUBTESTS()
SPLIT_FACTOR(8)

PEERDIR(
    library/python/testing/yatest_common
    ydb/tests/library
    ydb/tests/library/clients
)

END()
