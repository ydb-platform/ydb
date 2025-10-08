YQL_UDF_CONTRIB(histogram_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    ENABLE(YQL_STYLE_CPP)

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

