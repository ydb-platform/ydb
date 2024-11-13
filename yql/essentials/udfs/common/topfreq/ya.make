IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319906760 OUT_NOAUTO libtopfreq_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(topfreq_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        topfreq_udf.cpp
    )
    
    PEERDIR(
        yql/essentials/udfs/common/topfreq/static
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
    ut
)


