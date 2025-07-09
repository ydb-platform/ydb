YQL_UDF_CONTRIB(unicode_udf)
    
    YQL_ABI_VERSION(
        2
        37
        0
    )
    
    SRCS(
        unicode_base.cpp
    )
    
    PEERDIR(
        yql/essentials/udfs/common/unicode_base/lib
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)

