IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319903801 OUT_NOAUTO libset_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(set_udf)
    
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
