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

RECURSE_FOR_TESTS(
    test
)
