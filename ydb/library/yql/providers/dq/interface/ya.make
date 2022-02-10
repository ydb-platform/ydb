LIBRARY() 
 
OWNER(g:yql)
 
SRCS( 
    yql_dq_integration.cpp 
    yql_dq_task_preprocessor.cpp
    yql_dq_task_transform.cpp 
) 
 
PEERDIR( 
    contrib/libs/protobuf
    library/cpp/yson 
    ydb/library/yql/ast
    ydb/library/yql/core
) 
 
YQL_LAST_ABI_VERSION() 

END() 
