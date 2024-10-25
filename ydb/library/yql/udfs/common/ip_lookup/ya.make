IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libip_lookup_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(ip_lookup_udf)
    
    YQL_ABI_VERSION(
        2
        37
        0
    )
    
    SRCS(
        ip_lookup_udf.cpp
    )
    
    PEERDIR(
        ydb/library/yql/public/udf/arrow
        library/cpp/ipmath
        library/cpp/ipv6_address
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    tests
)


