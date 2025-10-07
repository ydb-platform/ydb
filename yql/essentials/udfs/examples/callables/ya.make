YQL_UDF_CONTRIB(callables_udf)
YQL_ABI_VERSION(2 38 0)

ENABLE(YQL_STYLE_CPP)

SRCS(
    callables_udf.cpp
)

END()

RECURSE_FOR_TESTS(
    test
)
