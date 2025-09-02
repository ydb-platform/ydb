PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_example.py  # TODO: change file name to yours
)

SIZE(MEDIUM)


PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
)

END()
