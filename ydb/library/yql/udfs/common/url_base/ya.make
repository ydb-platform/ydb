IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            liburl_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(url_udf)
    
    YQL_ABI_VERSION(
        2
        37
        0
    )
    
    SRCS(
        url_base.cpp
    )
    
    PEERDIR(
        ydb/library/yql/public/udf
        ydb/library/yql/udfs/common/url_base/lib
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)


