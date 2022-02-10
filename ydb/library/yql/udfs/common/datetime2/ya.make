YQL_UDF(datetime2_udf) 
 
YQL_ABI_VERSION( 
    2 
    24
    0 
) 
 
OWNER(
    g:yql
    g:yql_ydb_core
)
 
SRCS( 
    datetime_udf.cpp 
) 
 
PEERDIR( 
    util/draft 
    ydb/library/yql/minikql
    ydb/library/yql/public/udf/tz
) 
 
END() 
