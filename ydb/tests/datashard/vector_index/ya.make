PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

FORK_SUBTESTS()
SPLIT_FACTOR(38)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

TEST_SRCS(
    test_vector_index.py

)

PEERDIR(
    ydb/tests/datashard/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

END()
