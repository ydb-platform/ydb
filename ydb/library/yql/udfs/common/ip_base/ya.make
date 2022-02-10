YQL_UDF(ip_udf)

YQL_ABI_VERSION(
    2
    9
    0
)

OWNER(g:yql g:yql_ydb_core)

SRCS(
    ip_base.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/ip_base/lib 
)

END()
