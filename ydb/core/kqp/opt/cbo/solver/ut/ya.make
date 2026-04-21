UNITTEST_FOR(ydb/core/kqp/opt/cbo/solver)

SRCS(
    kqp_cbo_ut.cpp
    kqp_opt_hypergraph_ut.cpp
    kqp_opt_interesting_orderings_ut.cpp
)

PEERDIR(
    ydb/core/kqp/opt/cbo/solver
    ydb/core/kqp/opt/cbo
    yql/essentials/providers/common/provider
    yql/essentials/sql/pg
    yql/essentials/parser/pg_wrapper
    yql/essentials/public/udf/service/stub
)

SIZE(SMALL)

YQL_LAST_ABI_VERSION()

END()
