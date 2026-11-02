DEPENDS(
    library/recipes/docker_compose
    library/recipes/docker_compose/bin
)

USE_RECIPE(
    library/recipes/docker_compose/docker_compose --compose-file $DOCKER_COMPOSE_FILE --recipe-config $RECIPE_CONFIG_FILE --context-file $DOCKER_CONTEXT_FILE --test-host $DOCKER_TEST_HOST
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/large.inc)

DEFAULT(DOCKER_COMPOSE_TEST_CPU all)
DEFAULT(DOCKER_COMPOSE_TEST_CONTAINER 4467981730)

REQUIREMENTS(
    # docker tests usually cannot work simultaneously: cpu:all does not allow other tests to work in parallel
    cpu:${DOCKER_COMPOSE_TEST_CPU}
    # docker compose works well only with dns64
    dns:dns64
    # use default bionic lxc container with docker and allowed fuse
    # for more info see https://docs.yandex-team.ru/devtools/test/environment#docker-compose
    container:${DOCKER_COMPOSE_TEST_CONTAINER}
)
