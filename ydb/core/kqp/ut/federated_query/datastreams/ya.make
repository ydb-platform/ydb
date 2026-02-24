UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    library/cpp/threading/local_executor
    ydb/core/cms/console
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/core/testlib
    ydb/library/testlib/common
    ydb/library/testlib/pq_helpers
    ydb/library/testlib/s3_recipe_helper
    ydb/library/testlib/solomon_helpers
    ydb/library/yql/providers/generic/connector/libcpp
    ydb/library/yql/providers/generic/connector/libcpp/ut_helpers
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/tests/tools/s3_recipe/recipe.inc)
INCLUDE(${ARCADIA_ROOT}/ydb/library/yql/tools/solomon_emulator/recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
