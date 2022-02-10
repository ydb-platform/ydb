LIBRARY() 
 
YQL_ABI_VERSION( 
    2 
    9 
    0 
) 
 
OWNER(
    g:yql
    g:yql_ydb_core
)
 
SRCS( 
    ip_base_udf.cpp 
) 
 
PEERDIR( 
    ydb/library/yql/public/udf
) 
 
END() 
