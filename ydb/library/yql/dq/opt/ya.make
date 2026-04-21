LIBRARY()

PEERDIR(
    ydb/library/yql/dq/opt/core
    yql/essentials/core/cbo
    ydb/core/kqp/expr_nodes
)

SRCS(
    dq_opt_conflict_rules_collector.cpp
    dq_opt_join.cpp
    dq_opt_join_cbo_factory.cpp
    dq_opt_join_cost_based.cpp
    dq_opt_join_tree_node.cpp
    dq_opt_stat.cpp
    dq_opt_stat_transformer_base.cpp
    dq_opt_predicate_selectivity.cpp
)

CHECK_DEPENDENT_DIRS(DENY PEERDIRS
    ydb/core/kqp/opt/cbo
    ydb/core/kqp/opt/cbo/solver
)

YQL_LAST_ABI_VERSION()

END()

RECURSE(core)

RECURSE_FOR_TESTS(ut)
