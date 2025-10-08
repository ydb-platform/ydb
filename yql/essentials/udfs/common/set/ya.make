YQL_UDF_CONTRIB(set_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )

    ENABLE(YQL_STYLE_CPP)
    
    SRCS(
        set_udf.cpp
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)
