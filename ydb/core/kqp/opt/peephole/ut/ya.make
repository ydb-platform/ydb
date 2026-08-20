UNITTEST_FOR(ydb/core/kqp/opt/peephole)

SIZE(SMALL)

SRCS(
    kqp_opt_peephole_wasm_resident_ut.cpp
)

PEERDIR(
    ydb/core/kqp/expr_nodes
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
