PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)

TEST_SRCS(
    test_set_not_null_concurrency.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(cpu:2)
ENDIF()

FORK_SUBTESTS()
SPLIT_FACTOR(5)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
