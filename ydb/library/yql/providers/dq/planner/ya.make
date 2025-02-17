LIBRARY()

PEERDIR(
    yql/essentials/core/services
    yql/essentials/minikql/comp_nodes
    yql/essentials/core/dq_integration
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/dq/tasks
    yql/essentials/providers/common/mkql
    ydb/library/yql/providers/dq/api/protos
    ydb/library/yql/providers/dq/common
    ydb/library/yql/providers/dq/expr_nodes
    ydb/library/yql/providers/dq/mkql
    ydb/library/yql/providers/dq/opt
)

SRCS(
    dqs_task_graph.cpp
    execution_planner.cpp
)

YQL_LAST_ABI_VERSION()

END()
