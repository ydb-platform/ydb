UNITTEST_FOR(ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
REQUIREMENTS(cpu:1)
ENDIF()

SRCS(
    kqp_federated_query_ut.cpp
    kqp_federated_scheme_ut.cpp
    kqp_s3_plan_ut.cpp
    s3_recipe_ut_helpers.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/providers/s3/actors
    ydb/library/yql/sql/pg_dummy
    ydb/library/testlib/s3_recipe_helper
    ydb/public/sdk/cpp/client/ydb_types/operation
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

END()
