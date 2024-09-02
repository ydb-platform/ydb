PY3TEST()

STYLE_PYTHON()

NO_CHECK_IMPORTS()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/fq_runner/ydb_runner_with_datastreams.inc)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/mdb_mock/recipe.inc)

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/token_accessor_mock/recipe.inc)

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

ENV(COMPOSE_HTTP_TIMEOUT=1200)  # during parallel tests execution there could be huge disk io, which triggers timeouts in docker-compose 
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

IF (OPENSOURCE)
    IF (SANITIZER_TYPE)
        # Too huge for precommit check with sanitizers
        SIZE(LARGE)
    ELSE()
        # Including of docker_compose/recipe.inc automatically converts these tests into LARGE, 
        # which makes it impossible to run them during precommit checks on Github CI. 
        # Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.
        SIZE(MEDIUM)
    ENDIF()
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
    # This requirement forces tests to be launched consequently,
    # otherwise CI system would be overloaded due to simultaneous launch of many Docker containers.
    # See DEVTOOLSSUPPORT-44103, YA-1759 for details.
    TAG(ya:not_autocheck)
ENDIF()

DEPENDS(
    ydb/tests/tools/pq_read
)

PEERDIR(
    ydb/tests/fq/generic/utils
    ydb/tests/tools/datastreams_helpers
    library/python/testing/recipe
    library/python/testing/yatest_common
    library/recipes/common
    ydb/tests/tools/fq_runner
    ydb/public/api/protos
    contrib/python/pytest
)

TEST_SRCS(
    conftest.py
    test_clickhouse.py
    test_greenplum.py
    test_join.py
    test_postgresql.py
    test_streaming_join.py
    test_ydb.py
)

END()
