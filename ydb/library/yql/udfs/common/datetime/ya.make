IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(
        FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libdatetime_udf.so
    )

    END()
ELSE()
    YQL_UDF_YDB(datetime_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        datetime_udf.cpp
    )

    PEERDIR(
        library/cpp/timezone_conversion
        util/draft
    )
    END()

ENDIF()

RECURSE_FOR_TESTS(
    test
)