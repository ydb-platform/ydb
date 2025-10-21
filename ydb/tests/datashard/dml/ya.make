PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(23)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_dml.py

)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
