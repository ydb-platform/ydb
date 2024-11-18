YQL_UDF_CONTRIB(hyperloglog_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        hyperloglog_udf.cpp
    )

    PEERDIR(
        library/cpp/hyperloglog
    )

    END()

RECURSE_FOR_TESTS(
    test
)