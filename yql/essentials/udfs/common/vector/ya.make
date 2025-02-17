YQL_UDF_CONTRIB(vector_udf)

YQL_ABI_VERSION(
    2
    35
    0
)

SRCS(
    vector_udf.cpp
)

END()

RECURSE_FOR_TESTS(
    test
)
