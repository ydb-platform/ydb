SUBSCRIBER(g:kikimr)

PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_disk.py
    test_tablet.py
)

SIZE(MEDIUM)


DEPENDS(
)

PEERDIR(
    ydb/tests/tools/nemesis/library
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
