YQL_UDF(json_udf)
 
YQL_ABI_VERSION(
    2
    9
    0
)
 
OWNER(g:yql g:yql_ydb_core)

SRCS( 
    json_udf.cpp
) 
 
PEERDIR(
    library/cpp/json/easy_parse
)

END() 
