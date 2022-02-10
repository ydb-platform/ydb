LIBRARY() 
 
OWNER(g:yql)
 
PEERDIR( 
    ydb/library/yql/utils/log
    ydb/library/yql/dq/opt
    ydb/library/yql/dq/type_ann
    ydb/library/yql/providers/common/transform
    ydb/library/yql/providers/dq/expr_nodes
) 
 
SRCS( 
    dqs_opt.cpp 
    logical_optimize.cpp
    physical_optimize.cpp
) 
 
YQL_LAST_ABI_VERSION()

END() 
