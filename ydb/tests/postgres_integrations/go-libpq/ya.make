PY3TEST()

FORK_TEST_FILES()


# copy from https://docs.yandex-team.ru/devtools/test/environment#docker-compose
REQUIREMENTS(
    container:4467981730 # container with docker
    cpu:all dns:dns64
)

SET(ARCADIA_SANDBOX_SINGLESLOT TRUE)

IF(OPENSOURCE)
    IF (SANITIZER_TYPE)
        # Too huge for precommit check with sanitizers
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    ELSE()
        SIZE(MEDIUM) # for run per PR
    ENDIF()

    # Including of docker_compose/recipe.inc automatically converts these tests into LARGE,
    # which makes it impossible to run them during precommit checks on Github CI.
    # Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.

    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
    # This requirement forces tests to be launched consequently,
    # otherwise CI system would be overloaded due to simultaneous launch of many Docker containers.
    # See DEVTOOLSSUPPORT-44103, YA-1759 for details.
    TAG(ya:not_autocheck)
ELSE()
    SIZE(LARGE) # run in sandbox with timeout more than a minute
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    TAG(
        ya:external
        ya:force_sandbox
    )
ENDIF()


INCLUDE(${ARCADIA_ROOT}/ydb/tests/ydbd_dep.inc)
ENV(YDB_ALLOCATE_PGWIRE_PORT="true")
DEPENDS(
)

TEST_SRCS(
    conftest.py
    docker_wrapper_test.py
)


DATA(
    arcadia/ydb/tests/postgres_integrations/go-libpq/data
)

PEERDIR(
    ydb/tests/postgres_integrations/library
)

END()
