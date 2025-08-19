PY3TEST()
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
    ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
    ENV(YDB_ENABLE_COLUMN_TABLES="true")

    TEST_SRCS(
        order_by_with_limit.py
<<<<<<< HEAD
        test_quota_exhaustion.py
=======
        tablets_movement.py
        test_cs_many_updates.py
        test_log_scenario.py
        upgrade_to_internal_path_id.py
>>>>>>> 8753f2b29a5 (tablets movement test has been added (#23054))
        zip_bomb.py
    )

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
    )
END()

RECURSE(
    lib
    scenario
    docs
    load
    ttl_tiering
)
