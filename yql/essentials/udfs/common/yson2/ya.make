YQL_UDF_CONTRIB(yson2_udf)
    
    YQL_ABI_VERSION(
        2
        43
        0
    )

    SRCS(
        yson2_udf.cpp
    )
    
    PEERDIR(
        library/cpp/containers/stack_vector
        library/cpp/yson_pull
        yql/essentials/minikql/dom
        yql/essentials/public/langver
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)

