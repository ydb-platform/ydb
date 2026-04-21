LIBRARY()

PEERDIR(
    ydb/core/kqp/opt/cbo
    yql/essentials/ast
    yql/essentials/core
    ydb/library/yql/dq/common
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt/core
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/dq/expr_nodes
    ydb/core/kqp/expr_nodes
    yql/essentials/core/dq_integration
    ydb/library/yql/dq/proto
)

SRCS(
    kqp_opt_cbo_latency_predictor.cpp
    kqp_opt_conflict_rules_collector.cpp
    kqp_opt_join.cpp
    kqp_opt_join_cbo_factory.cpp
    kqp_opt_join_cost_based.cpp
    kqp_opt_join_tree_node.cpp
    kqp_opt_predicate_selectivity.cpp
    kqp_opt_stat.cpp
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(ut)
