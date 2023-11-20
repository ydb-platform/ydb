# Because of this: https://a.yandex-team.ru/arcadia/library/recipes/clickhouse/recipe/recipe.inc?blame=true&rev=r11609149#L2
IF (NOT OS_WINDOWS)

PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

SIZE(MEDIUM)

INCLUDE(${ARCADIA_ROOT}/library/recipes/clickhouse/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/library/recipes/postgresql/recipe.inc)
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
)

END()

ENDIF()
