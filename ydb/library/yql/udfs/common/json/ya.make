IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libjson_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(json_udf)

    YQL_ABI_VERSION(
        2
        28
        0
    )

    SRCS(
        json_udf.cpp
    )

    PEERDIR(
        library/cpp/json/easy_parse
    )

    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)
