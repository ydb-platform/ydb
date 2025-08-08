PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        test_log_scenario.py
        upgrade_to_internal_path_id.py
        zip_bomb.py
        test_cs_simple_reader_many_modifications.py
    )
    FORK_SUBTESTS()

    IF (SANITIZER_TYPE OR WITH_VALGRIND)
        SIZE(LARGE)
        TAG(ya:fat)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()

    DEPENDS(
        ydb/apps/ydb
        )

    PEERDIR(
        ydb/tests/library
        ydb/tests/library/test_meta
        ydb/tests/olap/common
        ydb/tests/olap/lib
    )
END()

RECURSE(
    column_family
    common
    docs
    high_load
    lib
    load
    oom
    s3_import
    scenario
    ttl_tiering
    data_quotas
    delete
)
