UNITTEST_FOR(ydb/core/external_sources/s3)

NO_CHECK_IMPORTS()

DATA(arcadia/ydb/core/external_sources/s3/ut/docker-compose.yml)
ENV(COMPOSE_PROJECT_NAME=s3)

IF (AUTOCHECK) 
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
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)

    # This requirement forces tests to be launched consequently,
    # otherwise CI system would be overloaded due to simultaneous launch of many Docker containers.
    # See DEVTOOLSSUPPORT-44103, YA-1759 for details.
    TAG(ya:not_autocheck)
    REQUIREMENTS(cpu:all)
ENDIF()

SRCS(
    s3_aws_credentials_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    library/cpp/testing/common
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/sql/pg_dummy
    ydb/public/sdk/cpp/client/ydb_types/operation
    ydb/library/actors/core
)

DEPENDS(
    library/recipes/docker_compose/bin
)

YQL_LAST_ABI_VERSION()

END()
