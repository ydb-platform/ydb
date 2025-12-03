YQL_UDF_YDB(statistics_internal_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    all_agg_funcs.cpp
    all_agg_funcs.h
    udf.cpp
)

PEERDIR(
    yql/essentials/core/minsketch
)

END()

RECURSE_FOR_TESTS(
    test
)
