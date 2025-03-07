PY3TEST()

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")

TEST_SRCS(
    conftest.py
    test_create_users.py
    test_create_users_strict_acl_checks.py
    test_db_counters.py
    test_dynamic_tenants.py
    test_tenants.py
    test_storage_config.py
    test_system_views.py
    test_auth_system_views.py
    test_publish_into_schemeboard_with_common_ssring.py
    test_users_groups_with_acl.py
)

SPLIT_FACTOR(20)

DEPENDS(
    ydb/apps/ydbd
)

PEERDIR(
    contrib/python/requests
    ydb/tests/library
    ydb/tests/library/fixtures
    ydb/tests/library/clients
    ydb/tests/oss/ydb_sdk_import
    ydb/public/sdk/python
)

FORK_SUBTESTS()

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:10 cpu:1)
ELSE()
    SIZE(MEDIUM)
ENDIF()

END()
