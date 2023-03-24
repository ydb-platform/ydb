UNITTEST_FOR(ydb/core/kqp)

IF (WITH_VALGRIND)
    TIMEOUT(3600)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

SRCS(
    kqp_federated_query_ut.cpp
)

PEERDIR(
    contrib/libs/aws-sdk-cpp/aws-cpp-sdk-s3
    ydb/core/kqp/ut/common
    ydb/library/yql/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)

END()
