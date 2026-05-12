YQL_UDF(linear_udf)
YQL_ABI_VERSION(2 44 0)

SRCS(
    linear_udf.cpp
)

END()

RECURSE_FOR_TESTS(
    test
)
