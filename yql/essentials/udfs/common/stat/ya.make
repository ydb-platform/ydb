YQL_UDF_CONTRIB(stat_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )

    ENABLE(YQL_STYLE_CPP)
    
    SRCS(
        stat_udf.cpp
    )
    
    PEERDIR(
        yql/essentials/udfs/common/stat/static
    )
    
    END()

IF (NOT EXPORT_CMAKE)
    RECURSE_FOR_TESTS(
        test
        ut
    )
ENDIF()
