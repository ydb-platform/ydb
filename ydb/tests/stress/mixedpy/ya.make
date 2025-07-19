PY3TEST()
INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")


TEST_SRCS(
    test_mixed.py
)

IF (SANITIZER_TYPE)
    REQUIREMENTS(ram:32)
ELSE()
    REQUIREMENTS(ram:16)
ENDIF()

TIMEOUT(1200)
SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    ydb/tests/library
    ydb/tests/library/stress
    ydb/tests/olap/lib
)


END()
