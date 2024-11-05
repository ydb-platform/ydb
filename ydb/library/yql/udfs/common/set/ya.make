IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libset_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(set_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        set_udf.cpp
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)
