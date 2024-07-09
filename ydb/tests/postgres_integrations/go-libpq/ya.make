PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)
SIZE(LARGE)

TAG(
    ya:fat
)

ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
DEPENDS(
    ydb/apps/ydbd
)

TEST_SRCS(
    conftest.py
    docker_wrapper_test.py
)


DATA(
    arcadia/ydb/tests/postgres_integrations/go-libpq/data
)

PEERDIR(
    ydb/tests/postgres_integrations/library
)

REQUIREMENTS(ram:10)

END()
