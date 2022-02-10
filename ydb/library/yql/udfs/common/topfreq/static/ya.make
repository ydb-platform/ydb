LIBRARY()

YQL_ABI_VERSION(
    2
    10
    0
)

OWNER( 
    g:yql 
    g:yql_ydb_core 
) 

SRCS(
    static_udf.cpp
    topfreq.cpp
)

PEERDIR(
    ydb/library/yql/public/udf 
)

END()
