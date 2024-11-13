IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319906274 OUT_NOAUTO libtop_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(top_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        top_udf.cpp
    )
    
    PEERDIR(
        library/cpp/containers/top_keeper
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)

