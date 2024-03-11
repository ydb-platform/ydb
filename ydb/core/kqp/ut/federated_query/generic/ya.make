UNITTEST_FOR(ydb/core/kqp)

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

SRCS(
    ch_recipe_ut_helpers.cpp
    connector_recipe_ut_helpers.cpp
    kqp_generic_plan_ut.cpp
    kqp_generic_provider_join_ut.cpp
    pg_recipe_ut_helpers.cpp
)

PEERDIR(
    contrib/libs/fmt
    contrib/libs/libpqxx
    library/cpp/clickhouse/client
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/sql/pg_dummy
)

INCLUDE(${ARCADIA_ROOT}/library/recipes/docker_compose/recipe.inc)

# Including of docker_compose/recipe.inc automatically converts these tests into LARGE, 
# which makes it impossible to run them during precommit checks on Github CI. 
# Next several lines forces these tests to be MEDIUM. To see discussion, visit YDBOPS-8928.

IF (OPENSOURCE)
    SIZE(MEDIUM)
    SET(TEST_TAGS_VALUE)
    SET(TEST_REQUIREMENTS_VALUE)
ENDIF()

YQL_LAST_ABI_VERSION()

END()
