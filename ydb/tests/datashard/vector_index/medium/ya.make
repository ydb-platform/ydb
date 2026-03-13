PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)

FORK_SUBTESTS()
SPLIT_FACTOR(39)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    REQUIREMENTS(cpu:4)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
    REQUIREMENTS(cpu:2)
ENDIF()

TEST_SRCS(
    test_vector_index.py
    test_vector_index_negative.py
)

PEERDIR(
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
)

END()
