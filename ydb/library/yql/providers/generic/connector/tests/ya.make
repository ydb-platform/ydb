PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

DATA(arcadia/ydb/library/yql/providers/generic/connector/tests/docker-compose.yml)

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

# Including of docker_compose/recipe.inc automatically converts these tests into LARGE, 
# which makes it impossible to run them during precommit checks on Github CI. 
# Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.

IF (OPENSOURCE)
    SIZE(MEDIUM)
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
ENDIF()

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
    library/recipes/docker_compose/bin
)

END()

RECURSE_FOR_TESTS(
    test_cases
    utils
)
