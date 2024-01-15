OWNER(g:yq)

PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

TAG(
    ya:external
    ya:force_sandbox
    ya:fat
)

REQUIREMENTS(
    container:4467981730
    cpu:all
    dns:dns64
)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/mdb_mock/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/token_accessor_mock/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

# Including of docker_compose/recipe.inc automatically converts these tests into LARGE,
# which makes it impossible to run them during precommit checks on Github CI.
# Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.

IF (OPENSOURCE)
    SIZE(MEDIUM)
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
ENDIF()

PEERDIR(
    ydb/tests/fq/generic/utils

    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/tools/fq_runner
    ydb/tests/tools/docker_compose_helpers
    ydb/public/api/protos

    contrib/python/pytest
)

TEST_SRCS(
    conftest.py
    test_clickhouse.py
    test_join.py
    test_postgresql.py
)

END()
