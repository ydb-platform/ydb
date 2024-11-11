YQL_UDF(topfreq_udf)
    
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

RECURSE_FOR_TESTS(
    test
    ut
)


