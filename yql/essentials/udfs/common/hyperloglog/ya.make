IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE 7319897411 OUT_NOAUTO libhyperloglog_udf.so
    )

    END()
ELSE()
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

ENDIF()

RECURSE_FOR_TESTS(
    test
)