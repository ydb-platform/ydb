PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    conftest.py
    test_db_counters.py
    test_dynamic_tenants.py
    test_tenants.py
    test_storage_config.py
    test_system_views.py
    test_publish_into_schemeboard_with_common_ssring.py
)

SPLIT_FACTOR(20)
TIMEOUT(600)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    contrib/python/requests
    ydb/tests/library
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

FORK_SUBTESTS()

REQUIREMENTS(ram:10)

END()
