IF (YQL_PACKAGED)
    PACKAGE()
        FROM_SANDBOX(FILE 7319908145 OUT_NOAUTO liburl_udf.so
        )
    END()
ELSE ()
YQL_UDF_CONTRIB(url_udf)
    
    YQL_ABI_VERSION(
        2
        37
        0
    )
    
    SRCS(
        url_base.cpp
    )
    
    PEERDIR(
        yql/essentials/public/udf
        yql/essentials/udfs/common/url_base/lib
    )
    
    END()
ENDIF ()


RECURSE_FOR_TESTS(
    test
)


