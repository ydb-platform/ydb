LIBRARY()

PEERDIR(
    ydb/library/yql/core
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
    ydb/library/yql/dq/type_ann
    ydb/library/yql/ast
)

SRCS(
    dq_task_program.cpp
)


   YQL_LAST_ABI_VERSION()


END()
