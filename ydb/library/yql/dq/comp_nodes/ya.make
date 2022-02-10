LIBRARY() 
 
OWNER( 
    g:yql 
) 
 
PEERDIR( 
    library/cpp/actors/core 
    ydb/library/yql/minikql/computation 
    ydb/library/yql/minikql 
    ydb/library/yql/dq/actors/compute 
) 
 
SRCS( 
    yql_common_dq_factory.cpp 
    yql_common_dq_transform.cpp 
) 
 
YQL_LAST_ABI_VERSION() 
 
 
END() 
 
