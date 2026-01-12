PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
SIZE(MEDIUM)

TEST_SRCS(
    conftest.py
    test_canonical_requests.py
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/apps/dstool
    ydb/apps/dstool/lib
)

END()
