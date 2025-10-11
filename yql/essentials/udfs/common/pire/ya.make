YQL_UDF_CONTRIB(pire_udf)
    
    YQL_ABI_VERSION(
        2
        27
        0
    )

    ENABLE(YQL_STYLE_CPP)
    
    SRCS(
        pire_udf.cpp
    )
    
    PEERDIR(
        library/cpp/regex/pire
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)
