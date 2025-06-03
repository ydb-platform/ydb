PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")

FORK_TEST_FILES()
FORK_TESTS()
FORK_SUBTESTS()
SPLIT_FACTOR(10)

TEST_SRCS(
    test_example.py
    test_followers.py
    test_compatibility.py
    test_statistics.py
    test_data_type.py
    udf/test_datetime2.py
    udf/test_digest.py
)

REQUIREMENTS(cpu:8)
REQUIREMENTS(ram:16)
SIZE(LARGE)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
    ydb/tests/library/compatibility/binaries
)

PEERDIR(
    ydb/tests/library
    ydb/tests/datashard/lib
    ydb/tests/stress/simple_queue/workload
    ydb/tests/library/compatibility
)

END()
