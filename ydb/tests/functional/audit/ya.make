PY3TEST()

FORK_SUBTESTS()
FORK_TEST_FILES()
# It is necessary to run all tests
# in separate chunks because our
# audit log capture method is unreliable
# and therefore some tests may affect neighbouring ones
SPLIT_FACTOR(100)
SIZE(MEDIUM)

ENV(YDB_USE_IN_MEMORY_PDISKS=true)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

TEST_SRCS(
    conftest.py
    test_auditlog.py
)

DEPENDS(
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:10 cpu:1)
ENDIF()

END()
