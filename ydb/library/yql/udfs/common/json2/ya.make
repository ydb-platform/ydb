YQL_UDF(json2_udf) 
 
YQL_ABI_VERSION( 
    2 
    21
    0 
) 
 
OWNER(
    g:kikimr
    g:yql
    g:yql_ydb_core
)
 
SRCS( 
    json2_udf.cpp 
) 
 
PEERDIR( 
    ydb/library/binary_json
    ydb/library/yql/minikql/dom
    ydb/library/yql/minikql/jsonpath
) 
 
END() 
