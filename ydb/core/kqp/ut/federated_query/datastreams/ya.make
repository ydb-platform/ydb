UNITTEST_FOR(ydb/core/kqp)

FORK_SUBTESTS()
SPLIT_FACTOR(50)

SIZE(MEDIUM)

SRCS(
    datastreams_ut.cpp
)

PEERDIR(
    library/cpp/threading/local_executor
    ydb/core/kqp
    ydb/core/kqp/ut/common
    ydb/core/kqp/ut/federated_query/common
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
)

INCLUDE(${ARCADIA_ROOT}/ydb/public/tools/ydb_recipe/recipe.inc)

YQL_LAST_ABI_VERSION()

END()
