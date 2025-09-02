UNITTEST_FOR(ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
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
    ydb/library/testlib/s3_recipe_helper
    ydb/library/yql/providers/s3/actors
    ydb/public/sdk/cpp/src/client/types/operation
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/yson2
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

END()
