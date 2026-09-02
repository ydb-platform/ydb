PY3TEST()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/vm_metadata_emulator/recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/fq/streaming_common/iam_grpc_emulator/recipe/recipe.inc)

DATA(arcadia/ydb/library/yql/providers/generic/connector/tests/fq-connector-go)
ENV(COMPOSE_HTTP_TIMEOUT=1200)  # during parallel tests execution there could be huge disk io, which triggers timeouts in docker-compose
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

TEST_SRCS(
    test_iam_generic.py
    test_join.py
)

PY_SRCS(
    conftest.py
)

IF (SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    REQUIREMENTS(ram:20)
ELSE()
    REQUIREMENTS(ram:12)
ENDIF()

PEERDIR(
    ydb/tests/library
    ydb/tests/library/test_meta
    ydb/public/sdk/python
    ydb/public/sdk/python/enable_v3_new_behavior
    library/recipes/common
    ydb/tests/olap/common
    ydb/tests/tools/datastreams_helpers
    ydb/tests/fq/streaming_common
    yql/essentials/providers/common/proto
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/tests/fq/generic/utils
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/public/api/protos
    contrib/python/pytest
)

DEPENDS(
    ydb/apps/ydb
)

END()
