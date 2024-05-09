IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libhistogram_udf.so
    )

    END()
ELSE()
    YQL_UDF_YDB(histogram_udf)

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