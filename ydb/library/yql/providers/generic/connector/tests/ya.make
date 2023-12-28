PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

SIZE(MEDIUM)

IF (AUTOCHECK) 
    # Split tests to chunks only when they're running on different machines with distbuild,
    # otherwise this directive will slow down local test execution.
    # Look through https://st.yandex-team.ru/DEVTOOLSSUPPORT-39642 for more information.
    FORK_SUBTESTS()

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
ENDIF()

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

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
    ydb/library/yql/tools/dqrun
    ydb/tests/tools/kqprun
)

END()

RECURSE_FOR_TESTS(
    test_cases
    utils
)
