IF (YQL_PACKAGED)
    PACKAGE()

    FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO libdatetime2_udf.so)

    END()
ELSE()
    YQL_UDF_YDB(datetime2_udf)
    YQL_ABI_VERSION(
        2
        37
        0
    )
    SRCS(
        datetime_udf.cpp
    )
    PEERDIR(
        util/draft
        ydb/library/yql/public/udf/arrow
        ydb/library/yql/minikql
        ydb/library/yql/minikql/datetime
        ydb/library/yql/public/udf/tz
    )
    END()
ENDIF()

RECURSE_FOR_TESTS(
    test
)
