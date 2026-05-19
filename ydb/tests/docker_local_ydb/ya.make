PY3TEST()

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

    # Hygiene mirroring postgres_integrations/go-libpq/ya.make:
    # ensure recipe.inc-style pollution doesn't force tests to LARGE.
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
    # Serialize docker-heavy tests on shared CI runners.
    # See DEVTOOLSSUPPORT-44103, YA-1759.
    TAG(ya:not_autocheck)
ELSE()
    SIZE(LARGE) # run in sandbox with timeout more than a minute
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
    TAG(
        ya:external
        ya:force_sandbox
    )
ENDIF()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/harness_dep.inc)
DEPENDS(
    ydb/public/tools/local_ydb
)

TEST_SRCS(
    test_docker_image.py
)

DATA(
    arcadia/.github/docker
)

PEERDIR(
    contrib/python/docker
)

END()
