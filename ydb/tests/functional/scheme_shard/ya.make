PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    test_copy_ops.py
    test_alter_ops.py
    test_scheme_shard_operations.py
)

SIZE(MEDIUM)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
