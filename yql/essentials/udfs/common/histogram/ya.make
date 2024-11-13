IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE 7319896927 OUT_NOAUTO libhistogram_udf.so
    )

    END()
ELSE()
YQL_UDF_CONTRIB(histogram_udf)

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

ENDIF()

RECURSE_FOR_TESTS(
    test
)