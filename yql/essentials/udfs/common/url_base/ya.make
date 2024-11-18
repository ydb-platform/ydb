YQL_UDF_CONTRIB(url_udf)
    
    YQL_ABI_VERSION(
        2
        37
        0
    )
    
    SRCS(
        url_base.cpp
    )
    
    PEERDIR(
        yql/essentials/public/udf
        yql/essentials/udfs/common/url_base/lib
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)


