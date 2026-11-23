IF (OS_LINUX AND CLANG)
    YQL_UDF_MINITEST()
    DEPENDS(
        yql/essentials/udfs/common/hyperscan/impl
    )
    TIMEOUT(300)
    SIZE(MEDIUM)
    END()

ENDIF()
