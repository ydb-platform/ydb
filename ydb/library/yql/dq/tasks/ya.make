LIBRARY() 
 
OWNER( 
    g:yql g:yql_ydb_core
) 
 
PEERDIR( 
    ydb/library/yql/core
    ydb/library/yql/dq/expr_nodes
    ydb/library/yql/dq/proto
) 
 
SRCS( 
    dq_task_program.cpp
) 
 

   YQL_LAST_ABI_VERSION()


END() 
