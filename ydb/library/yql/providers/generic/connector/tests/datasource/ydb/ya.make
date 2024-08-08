PY3TEST()

NO_CHECK_IMPORTS()

DATA(
    arcadia/ydb/library/yql/providers/generic/connector/tests/datasource/ydb/docker-compose.yml
)

DATA(
    arcadia/ydb/library/yql/providers/generic/connector/tests/fq-connector-go
)

ENV(COMPOSE_PROJECT_NAME=ydb)

IF (AUTOCHECK)
    # YQ-3351: enabling python style checks only for opensource
    NO_LINT()
    # Temporarily disable these tests due to infrastructure incompatibility
    SKIP_TEST("DEVTOOLSUPPORT-44637")
    # Split tests to chunks only when they're running on different machines with distbuild,
    # otherwise this directive will slow down local test execution.
    # Look through DEVTOOLSSUPPORT-39642 for more information.
    FORK_SUBTESTS()
    # TAG and REQUIREMENTS are copied from: https://docs.yandex-team.ru/devtools/test/environment#docker-compose
    TAG(
        ya:external
        ya:force_sandbox
        ya:fat
    )
    REQUIREMENTS(
        cpu:all
        container:4467981730
        dns:dns64
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

IF (OPENSOURCE)
    # Including of docker_compose/recipe.inc automatically converts these tests into LARGE, 
    # which makes it impossible to run them during precommit checks on Github CI. 
    # Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
    # This requirement forces tests to be launched consequently,
    # otherwise CI system would be overloaded due to simultaneous launch of many Docker containers.
    # See DEVTOOLSSUPPORT-44103, YA-1759 for details.
    TAG(ya:not_autocheck)
    REQUIREMENTS(cpu:all)
ENDIF()

TEST_SRCS(
    select_positive.py
    collection.py
    conftest.py
    test.py
)

PEERDIR(
    contrib/python/pytest
    ydb/library/yql/providers/generic/connector/api/common
    ydb/library/yql/providers/generic/connector/tests/common_test_cases
    ydb/library/yql/providers/generic/connector/tests/utils
    ydb/library/yql/providers/generic/connector/tests/utils/run
    ydb/library/yql/providers/generic/connector/tests/utils/clients
    ydb/library/yql/providers/generic/connector/tests/utils/scenario
)

DEPENDS(
    ydb/library/yql/tools/dqrun
    ydb/library/yql/udfs/common/json2
    ydb/tests/tools/kqprun
    library/recipes/docker_compose/bin
)

END()
