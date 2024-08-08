PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    test_copy_ops.py
    test_alter_ops.py
    test_scheme_shard_operations.py
)

TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
