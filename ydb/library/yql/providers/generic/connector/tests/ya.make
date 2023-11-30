PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

SIZE(LARGE)

# TAG and REQUIREMENTS are copied from: https://docs.yandex-team.ru/devtools/test/environment#docker-compose
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

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/providers/generic/connector/recipe/recipe.inc)

FORK_SUBTESTS()

TEST_SRCS(
    conftest.py
    clickhouse.py
    postgresql.py
    test.py
)

PEERDIR(
    contrib/python/Jinja2
    contrib/python/clickhouse-connect
    contrib/python/grpcio
    contrib/python/pg8000
    contrib/python/pytest
    contrib/python/tzlocal
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/api/service
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/tests/test_cases
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/public/api/protos
    yt/python/yt/yson
)

DEPENDS(
    ydb/library/yql/providers/generic/connector/app
    ydb/library/yql/tools/dqrun
    ydb/tests/tools/kqprun
)

END()
