PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)

IF (AUTOCHECK)
    # copy from https://docs.yandex-team.ru/devtools/test/environment#docker-compose
    INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)
    SIZE(LARGE) # Docker можно запускать только в Sandbox, т.е. в LARGE тестах

    TAG(
        ya:external
        ya:fat
        ya:force_sandbox
    )

    REQUIREMENTS(
        container:4467981730 # В этом контейнере уже есть Docker и выполнена аутентификация в registry.yandex.net от имени пользователя arcadia-devtools
        cpu:all dns:dns64
    )
ENDIF()

IF(OPENSOURCE)
    SIZE(MEDIUM) # for run per PR
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
