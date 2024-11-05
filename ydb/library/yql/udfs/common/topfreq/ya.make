IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE {FILE_RESOURCE_ID} OUT_NOAUTO
            libtopfreq_udf.so
        )
    END()
ELSE ()
    YQL_UDF_YDB(topfreq_udf)
    
    YQL_ABI_VERSION(
        2
        28
        0
    )
    
    SRCS(
        topfreq_udf.cpp
    )
    
    PEERDIR(
        ydb/library/yql/udfs/common/topfreq/static
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
    ut
)


