UNITTEST_FOR(ydb/core/kqp/opt/cbo/solver)

SRCS(
    dq_cbo_ut.cpp
    dq_opt_hypergraph_ut.cpp
    dq_opt_interesting_orderings_ut.cpp
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
