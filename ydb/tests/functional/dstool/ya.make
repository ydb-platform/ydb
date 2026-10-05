PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
FORK_SUBTESTS()
SPLIT_FACTOR(2)
SIZE(MEDIUM)

REQUIREMENTS(cpu:4)

TEST_SRCS(
    conftest.py
    test_canonical_requests.py
    test_static_vdisk_evict.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/apps/dstool
    ydb/apps/dstool/lib
)

END()
