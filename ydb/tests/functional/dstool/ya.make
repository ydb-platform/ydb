PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
SIZE(MEDIUM)
<<<<<<< HEAD
REQUIREMENTS(cpu:4)
<<<<<<< HEAD
=======
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
>>>>>>> 8b9e2730796 (New command dstool group resize (#35643))
=======
>>>>>>> 965d96f48a3 (Fix and unmute dstool test_capacity_metrics (#35944))

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
