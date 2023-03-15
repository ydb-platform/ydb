YQL_UDF(topfreq_udf)

YQL_ABI_VERSION(
    2
    27
    0
)

SRCS(
    topfreq_udf.cpp
)

PEERDIR(
    ydb/library/yql/udfs/common/topfreq/static
)

END()

RECURSE_FOR_TESTS(
    ut
)
