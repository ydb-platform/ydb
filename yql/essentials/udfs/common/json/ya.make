IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319900360 OUT_NOAUTO libjson_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(json_udf)

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
