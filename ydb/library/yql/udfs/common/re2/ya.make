IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libre2_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(re2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        re2_udf.cpp
    )
    
    PEERDIR(
        contrib/libs/re2
        library/cpp/deprecated/enum_codegen
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)
