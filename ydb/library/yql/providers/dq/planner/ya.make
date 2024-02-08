LIBRARY()

PEERDIR(
    ydb/library/yql/core/services
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/dq/integration
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/dq/tasks
    ydb/library/yql/providers/common/mkql
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
