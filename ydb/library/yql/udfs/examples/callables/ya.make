YQL_UDF_YDB(callables_udf)
YQL_ABI_VERSION(2 36 0)

SRCS(
    callables_udf.cpp
)

END()

RECURSE_FOR_TESTS(
    test
)
