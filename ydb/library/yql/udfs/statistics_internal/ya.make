YQL_UDF_YDB(statistics_internal_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    udf.cpp
)

PEERDIR(
    ydb/core/statistics/agg_funcs
)

END()

RECURSE_FOR_TESTS(
    ut
)
