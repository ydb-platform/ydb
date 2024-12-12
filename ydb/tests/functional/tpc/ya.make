IF (NOT SANITIZER_TYPE)

PY3TEST()
ENV(YDB_HARD_MEMORY_LIMIT_BYTES="107374182400")

PY_SRCS(
    conftest.py
)

TEST_SRCS(
    test_tpch.py
)

SIZE(MEDIUM)

REQUIREMENTS(ram:16)

ENV(YDB_ENABLE_COLUMN_TABLES="true")
ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

PEERDIR(
    ydb/tests/library
    ydb/tests/olap/load/lib
)

DEPENDS(
    ydb/apps/ydb
    ydb/apps/ydbd
)

FORK_TEST_FILES()

END()

ENDIF()
