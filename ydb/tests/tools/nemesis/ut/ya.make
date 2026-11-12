SUBSCRIBER(g:kikimr)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_disk.py
    test_tablet.py
)

SIZE(MEDIUM)
REQUIREMENTS(cpu:4)

DEPENDS(
)

PEERDIR(
    ydb/tests/tools/nemesis/library
)

FORK_SUBTESTS()
SPLIT_FACTOR(5)

END()
