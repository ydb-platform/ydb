# Integration tests for YT Queue API with local YT in Docker.
# Uses docker-compose to spin up a YT cluster for each test run.
# Reproduces the scenario from:
# https://ytsaurus.tech/docs/ru/user-guide/dynamic-tables/queues#primer-ispolzovaniya

PY3TEST()

SET(DOCKER_COMPOSE_FILE ydb/tests/fq/yt/yt_integration/yt_in_docker/docker-compose.yml)

ENV(COMPOSE_HTTP_TIMEOUT=600)

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

PEERDIR(
    contrib/python/pytest
    library/python/testing/yatest_common
)

DATA(
    arcadia/ydb/tests/fq/yt/yt_integration/yt_in_docker
)

TEST_SRCS(
    test_queue_api.py
    yt_in_docker/__init__.py
    yt_in_docker/yt_client.py
)

END()
