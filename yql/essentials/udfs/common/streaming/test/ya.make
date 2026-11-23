IF (OS_LINUX)
    YQL_UDF_MINITEST()
    DEPENDS(
        yql/essentials/udfs/common/digest/impl
        yql/essentials/udfs/common/string
        yql/essentials/udfs/common/streaming/impl
    )
    TIMEOUT(300)
    SIZE(MEDIUM)
    END()

ENDIF()
