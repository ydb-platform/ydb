LIBRARY()

PEERDIR(
    ydb/core/kqp/opt/cbo
    yql/essentials/ast
    yql/essentials/core
    ydb/library/yql/dq/common
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/dq/expr_nodes
    ydb/core/kqp/expr_nodes
    yql/essentials/core/dq_integration
    yql/essentials/parser/pg_wrapper/interface
    ydb/library/yql/dq/proto
)

SRCS(
    dq_opt_conflict_rules_collector.cpp
    dq_opt_join.cpp
    dq_opt_join_cbo_factory.cpp
    dq_opt_join_cost_based.cpp
    dq_opt_join_tree_node.cpp
    dq_opt_stat.cpp
    dq_opt_stat_kqp.cpp
    dq_opt_stat_transformer_base.cpp
)

YQL_LAST_ABI_VERSION()

END()
