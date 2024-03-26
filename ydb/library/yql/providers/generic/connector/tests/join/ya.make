PY3TEST()

STYLE_PYTHON()
NO_CHECK_IMPORTS()

DATA(arcadia/ydb/library/yql/providers/generic/connector/tests/join/docker-compose.yml)
DATA(arcadia/ydb/library/yql/providers/generic/connector/tests/fq-connector-go)
ENV(COMPOSE_PROJECT_NAME=join)

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
    collection.py
    conftest.py
    scenario.py
    test.py
    test_case.py
)

PEERDIR(
    contrib/python/pytest
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/api/service/protos
    ydb/library/yql/providers/generic/connector/tests/test_cases
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/tests/utils/clients
    ydb/library/yql/providers/generic/connector/tests/utils/scenario
    ydb/library/yql/providers/generic/connector/tests/utils/types
)

DEPENDS(
    ydb/library/yql/tools/dqrun
    ydb/tests/tools/kqprun
    library/recipes/docker_compose/bin
)

END()
