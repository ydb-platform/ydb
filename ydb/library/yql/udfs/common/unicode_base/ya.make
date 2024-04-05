IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libunicode_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(unicode_udf)
    
    YQL_ABI_VERSION(
        2
        27
        0
    )
    
    SRCS(
        unicode_base.cpp
    )
    
    PEERDIR(
        ydb/library/yql/udfs/common/unicode_base/lib
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)

