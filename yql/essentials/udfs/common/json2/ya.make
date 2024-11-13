IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319901430 OUT_NOAUTO libjson2_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(json2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
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
ENDIF ()


RECURSE_FOR_TESTS(
    test
)

