YQL_UDF(unicode_udf)

YQL_ABI_VERSION(
    2
    9
    0
)

OWNER(g:yql g:yql_ydb_core)

SRCS(
    unicode_base.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/unicode_base/lib
)

END()
