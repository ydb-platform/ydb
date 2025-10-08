YQL_UDF(linear_udf)
YQL_ABI_VERSION(2 44 0)

ENABLE(YQL_STYLE_CPP)

SRCS(
    linear_udf.cpp
)

END()

RECURSE_FOR_TESTS(
    test
)
