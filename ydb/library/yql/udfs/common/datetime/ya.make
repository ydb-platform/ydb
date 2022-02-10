YQL_UDF(datetime_udf) 

YQL_ABI_VERSION(
    2
    9 
    0
)

OWNER(g:yql g:yql_ydb_core) 

SRCS(
    datetime_udf.cpp
)

PEERDIR(
    library/cpp/timezone_conversion
    util/draft
)

END()
