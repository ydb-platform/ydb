YQL_UDF_CONTRIB(json2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )

    ENABLE(YQL_STYLE_CPP)
    
    SRCS(
        json2_udf.cpp
    )
    
    PEERDIR(
        yql/essentials/core/sql_types
        yql/essentials/types/binary_json
        yql/essentials/minikql/dom
        yql/essentials/minikql/jsonpath
    )
    
    END()

RECURSE_FOR_TESTS(
    test
)

