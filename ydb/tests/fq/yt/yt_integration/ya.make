# Integration tests for QYT with local YT in Docker.
# Uses docker-compose to spin up a YT cluster for each test run.

PY3TEST()

SET(DOCKER_COMPOSE_FILE ydb/tests/fq/yt/yt_integration/yt_in_docker/docker-compose.yml)

ENV(COMPOSE_HTTP_TIMEOUT=600)

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

DEPENDS(ydb/apps/ydb)

PEERDIR(
    contrib/python/pytest
    library/python/testing/yatest_common
    ydb/tests/fq/streaming_common
    ydb/tests/tools/datastreams_helpers
    ydb/tests/library
    ydb/public/sdk/python
)

DATA(
    arcadia/ydb/tests/fq/yt/yt_integration/yt_in_docker
)

TEST_SRCS(
    test_queue_api.py
    test_qyt_read.py
    yt_in_docker/__init__.py
    yt_in_docker/yt_client.py
)

END()
