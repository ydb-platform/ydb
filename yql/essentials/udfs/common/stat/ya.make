IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319904307 OUT_NOAUTO libstat_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(stat_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        stat_udf.cpp
    )
    
    PEERDIR(
        yql/essentials/udfs/common/stat/static
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    ut
)

