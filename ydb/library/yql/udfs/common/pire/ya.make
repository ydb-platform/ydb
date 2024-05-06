IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libpire_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(pire_udf)
    
    YQL_ABI_VERSION(
        2
        27
        0
    )
    
    SRCS(
        pire_udf.cpp
    )
    
    PEERDIR(
        library/cpp/regex/pire
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)
