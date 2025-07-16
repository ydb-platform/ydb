PY3TEST()

TEST_SRCS(
    test_config_with_metadata.py
    test_generate_dynamic_config.py
    test_distconf_generate_config.py
    test_distconf_reassign_state_storage.py
    test_distconf.py
    test_config_migration.py
    test_configuration_version.py
)

SPLIT_FACTOR(10)

REQUIREMENTS(ram:32 cpu:32)
SIZE(LARGE)
TAG(ya:fat)

IF (SANITIZER_TYPE == "thread")
    TIMEOUT(1800)
ELSE()
    TIMEOUT(600)
ENDIF()


INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_CLI_BINARY="ydb/apps/ydb/ydb")
ENV(IAM_TOKEN="")
DEPENDS(
    ydb/apps/ydb
)

PEERDIR(
    contrib/python/requests
    ydb/tests/library
    ydb/tests/library/clients
)

FORK_SUBTESTS()
FORK_TEST_FILES()

END()
