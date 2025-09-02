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

RECURSE_FOR_TESTS(
    test
)

