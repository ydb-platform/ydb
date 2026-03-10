PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
SIZE(MEDIUM)
<<<<<<< HEAD
REQUIREMENTS(cpu:4)
=======
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
>>>>>>> 8b9e2730796 (New command dstool group resize (#35643))

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
