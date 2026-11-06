PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_copy_ops.py
    test_alter_ops.py
    test_scheme_shard_operations.py
)

SIZE(MEDIUM)
IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
)

# Reuse the class-scoped cluster for all tests in each file.
FORK_TEST_FILES()

END()
