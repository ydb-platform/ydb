UNITTEST_FOR(ydb/core/kqp)

IF (WITH_VALGRIND OR SANITIZER_TYPE)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_scriptexec_results_ut.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/library/yql/providers/s3/actors
    yql/essentials/sql/pg_dummy
    ydb/library/testlib/s3_recipe_helper
    ydb/public/sdk/cpp/src/client/types/operation
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

END()
