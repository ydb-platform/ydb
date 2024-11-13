IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319908881 OUT_NOAUTO libyson2_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(yson2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        yson2_udf.cpp
    )
    
    PEERDIR(
        library/cpp/containers/stack_vector
        library/cpp/yson_pull
        yql/essentials/minikql/dom
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)

