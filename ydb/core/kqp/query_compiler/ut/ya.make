UNITTEST_FOR(ydb/core/kqp/query_compiler)

SIZE(SMALL)

SRCS(
    kqp_wasm_string_columns_ut.cpp
)

PEERDIR(
    ydb/core/kqp/expr_nodes
    ydb/core/kqp/query_compiler
    yql/essentials/ast
    yql/essentials/core
    yql/essentials/core/expr_nodes
    yql/essentials/minikql
    yql/essentials/public/udf
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    library/cpp/testing/unittest
)

YQL_LAST_ABI_VERSION()

END()
