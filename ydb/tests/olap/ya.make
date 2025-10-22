PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        order_by_with_limit.py
        tablets_movement.py
        test_cs_many_updates.py
        test_log_scenario.py
        upgrade_to_internal_path_id.py
        data_read_correctness.py
        test_overloads.py
        zip_bomb.py
        test_create.py
        test_delete.py
        test_insert.py
        test_replace.py
        test_select.py
        test_update.py
        test_upsert.py
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
