PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)

IF (AUTOCHECK)
    # copy from https://docs.yandex-team.ru/devtools/test/environment#docker-compose
    INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)
    REQUIREMENTS(
        container:4467981730 # container with docker
        cpu:all dns:dns64
    )

ENDIF()

IF(OPENSOURCE)
    SIZE(MEDIUM) # for run per PR
ELSE()
    SIZE(LARGE) # run in sandbox with timeout more than a minute
    TAG(
        ya:external
        ya:fat
        ya:force_sandbox
    )
ENDIF()


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

END()
