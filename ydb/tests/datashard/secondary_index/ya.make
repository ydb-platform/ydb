PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(28)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_secondary_index.py

)

PEERDIR(
    ydb/tests/datashard/lib
    ydb/tests/sql/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
