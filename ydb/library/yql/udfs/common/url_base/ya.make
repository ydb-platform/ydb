YQL_UDF(url_udf)

YQL_ABI_VERSION(
    2
    23
    0
)

OWNER(
    g:yql
    g:yql_ydb_core
)

SRCS(
    url_base.cpp 
)

PEERDIR(
    ydb/library/yql/public/udf
    ydb/library/yql/udfs/common/url_base/lib
)

END()
