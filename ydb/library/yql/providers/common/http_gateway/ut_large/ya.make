UNITTEST_FOR(ydb/library/yql/providers/common/http_gateway)

DATA(arcadia/ydb/library/yql/providers/common/http_gateway/ut/docker-compose.yml)

ENV(COMPOSE_PROJECT_NAME=http_gateway)

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

ENV(COMPOSE_HTTP_TIMEOUT=1200)  # during parallel tests execution there could be huge disk io, which triggers timeouts in docker-compose 
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

IF (OPENSOURCE)
    # TODO: uncomment these lines when build infrastructure is fixed.
    #
    # IF (SANITIZER_TYPE)
    #     # Too huge for precommit check with sanitizers
    #     SIZE(LARGE)
    # ELSE()
    #     # Including of docker_compose/recipe.inc automatically converts these tests into LARGE, 
    #     # which makes it impossible to run them during precommit checks on Github CI. 
    #     # Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.
    #     SIZE(MEDIUM)
    # ENDIF()
    # SET(TEST_TAGS_VALUE)
    # SET(TEST_REQUIREMENTS_VALUE)
    
    # This requirement forces tests to be launched consequently,
    # otherwise CI system would be overloaded due to simultaneous launch of many Docker containers.
    # See DEVTOOLSSUPPORT-44103, YA-1759 for details.
    TAG(ya:not_autocheck)
ENDIF()

SRCS(
    yql_http_gateway_ut.cpp
)

PEERDIR(
    library/cpp/testing/common
    library/cpp/testing/hook
    library/cpp/testing/unittest
    library/cpp/threading/future
    ydb/core/kqp/ut/common
    ydb/core/util
    ydb/library/yql/providers/common/http_gateway
    yql/essentials/sql/pg_dummy
    yql/essentials/utils
)

DEPENDS(
    library/recipes/docker_compose/bin
)

YQL_LAST_ABI_VERSION()

END()
