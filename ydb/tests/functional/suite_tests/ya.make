IF (NOT SANITIZER_TYPE AND NOT WITH_VALGRIND)
    PY3TEST()
    ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
    ENV(USE_IN_MEMORY_PDISKS=true)
    TEST_SRCS(
        test_base.py
        test_postgres.py
        test_sql_logic.py
        test_stream_query.py
    )

    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

    DEPENDS(
        ydb/apps/ydbd
    )

    DATA (
        arcadia/ydb/tests/functional/suite_tests/postgres
        arcadia/ydb/tests/functional/suite_tests/sqllogictest
    )

    PEERDIR(
        ydb/tests/library
        ydb/tests/oss/canonical
        ydb/tests/oss/ydb_sdk_import
    )

    FORK_SUBTESTS()
    FORK_TEST_FILES()

    REQUIREMENTS(ram:12)

    END()
ENDIF()
