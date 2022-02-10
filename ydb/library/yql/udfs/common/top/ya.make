YQL_UDF(top_udf) 
 
YQL_ABI_VERSION( 
    2 
    10 
    0 
) 
 
OWNER(g:yql g:yql_ydb_core)
 
SRCS( 
    top_udf.cpp 
) 
 
PEERDIR( 
    library/cpp/containers/top_keeper
) 
 
END() 
