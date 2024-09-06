IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
PY3TEST()
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ERASURE=mirror_3_dc)
ENV(YDB_USE_IN_MEMORY_PDISKS=true)

TEST_SRCS(
    test_serializable.py
)

REQUIREMENTS(
    ram:32
)

TIMEOUT(1800)
SIZE(LARGE)
TAG(ya:fat)

DEPENDS(
    ydb/tests/tools/ydb_serializable
    ydb/apps/ydbd
)

PEERDIR(
    ydb/tests/library
)


END()

ENDIF()
