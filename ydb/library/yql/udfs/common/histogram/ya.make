YQL_UDF(histogram_udf)

YQL_ABI_VERSION(
    2
    9
    0
)

OWNER(g:yql g:yql_ydb_core)

SRCS(
    histogram_udf.cpp
)

PEERDIR(
    library/cpp/histogram/adaptive
)

END()
