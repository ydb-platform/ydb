UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

IF (SANITIZER_TYPE OR WITH_VALGRIND)
    SIZE(LARGE)
    INCLUDE(${ARCADIA_ROOT}/ydb/tests/large.inc)
ELSE()
    SIZE(MEDIUM)
ENDIF()

SRCS(
    datastreams_scheme_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    ydb/core/kqp/workload_service/ut/common
    ydb/core/tx/columnshard/hooks/testing
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
