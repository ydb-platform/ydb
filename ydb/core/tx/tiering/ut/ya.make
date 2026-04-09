UNITTEST_FOR(ydb/core/tx/tiering)

NO_CHECK_IMPORTS()

DATA(arcadia/ydb/core/tx/tiering/ut/docker-compose.yml)

ENV(COMPOSE_PROJECT_NAME=tiering_ut_minio)
ENV(TZ="UTC+13")

IF (AUTOCHECK)
    FORK_SUBTESTS()

    TAG(
        ya:external
        ya:force_sandbox
        ya:fat
    )

    REQUIREMENTS(
        cpu:4
        container:4467981730
        dns:dns64
    )
ENDIF()

ENV(COMPOSE_HTTP_TIMEOUT=1200)
INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

IF (OPENSOURCE)
    IF (SANITIZER_TYPE)
        SIZE(LARGE)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)

    TAG(ya:not_autocheck)
    REQUIREMENTS(cpu:4)
ELSE()
    IF (SANITIZER_TYPE == "thread")
        SIZE(LARGE)
        INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
        REQUIREMENTS(ram:16)
    ELSE()
        SIZE(MEDIUM)
    ENDIF()
ENDIF()

PEERDIR(
    library/cpp/getopt
    library/cpp/regex/pcre
    library/cpp/svnversion
    library/cpp/testing/common
    library/cpp/testing/unittest
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/olap/helpers
    ydb/core/testlib/default
    ydb/core/tx
    ydb/core/tx/columnshard
    ydb/core/tx/columnshard/hooks/testing
    ydb/core/tx/columnshard/test_helper
    ydb/library/actors/core
    ydb/public/lib/yson_value
    ydb/public/sdk/cpp/src/client/types/operation
    ydb/services/metadata
    yql/essentials/sql/pg_dummy
)

DEPENDS(
    library/recipes/docker_compose/bin
)

YQL_LAST_ABI_VERSION()

SRCS(
    olap_tiering_s3_secrets_ut.cpp
    ut_object.cpp
    ut_tiers.cpp
)

END()
