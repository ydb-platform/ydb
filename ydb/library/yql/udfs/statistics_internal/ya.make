YQL_UDF_YDB(statistics_internal_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    cms_agg_func.h
    ewh_agg_func.h
    all_agg_funcs.cpp
    all_agg_funcs.h
    common.h
    udf.cpp
)

PEERDIR(
    yql/essentials/core/minsketch
    yql/essentials/core/histogram
)

END()

RECURSE_FOR_TESTS(
    test
)
