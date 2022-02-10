YQL_UDF(stat_udf)

YQL_ABI_VERSION( 
    2 
    9
    0 
) 
 
OWNER(xenoxeno g:yql g:yql_ydb_core)

SRCS(
    stat_udf.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/stat/static
)

END()

RECURSE_FOR_TESTS(
    ut
)
