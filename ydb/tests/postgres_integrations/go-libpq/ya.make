PY3TEST()

FORK_TEST_FILES()
TIMEOUT(600)


# copy from https://docs.yandex-team.ru/devtools/test/environment#docker-compose
REQUIREMENTS(
    container:4467981730 # container with docker
    cpu:all dns:dns64
)

IF(OPENSOURCE)
    SIZE(MEDIUM) # for run per PR

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
    TAG(
        ya:external
        ya:fat
        ya:force_sandbox
    )
ENDIF()


ENV(YDB_DRIVER_BINARY="ydb/apps/ydbd/ydbd")
ENV(YDB_ALLOCATE_PGWIRE_PORT="true")
DEPENDS(
    ydb/apps/ydbd
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
