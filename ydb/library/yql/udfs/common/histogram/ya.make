YQL_UDF(histogram_udf)

YQL_ABI_VERSION(
    2
    28
    0
)

SRCS(
    histogram_udf.cpp
)

PEERDIR(
    library/cpp/histogram/adaptive
)

END()

RECURSE_FOR_TESTS(
    test
)