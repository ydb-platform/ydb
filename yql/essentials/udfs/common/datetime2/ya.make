IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(FILE 7319895543 OUT_NOAUTO libdatetime2_udf.so)

    END()
ELSE()
YQL_UDF_CONTRIB(datetime2_udf)
    YQL_ABI_VERSION(
        2
        40
        0
    )
    SRCS(
        datetime_udf.cpp
    )
    PEERDIR(
        util/draft
        yql/essentials/public/udf/arrow
        yql/essentials/minikql
        yql/essentials/minikql/datetime
        yql/essentials/public/udf/tz
    )
    END()
ENDIF()

RECURSE_FOR_TESTS(
    test
    test_bigdates
)
