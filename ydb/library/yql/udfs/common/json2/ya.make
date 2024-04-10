IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libjson2_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(json2_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        json2_udf.cpp
    )
    
    PEERDIR(
        ydb/library/binary_json
        ydb/library/yql/minikql/dom
        ydb/library/yql/minikql/jsonpath
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)

