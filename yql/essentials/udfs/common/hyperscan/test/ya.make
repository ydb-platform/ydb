IF (OS_LINUX AND CLANG)

YQL_UDF_MINITEST()

DEPENDS(yql/essentials/udfs/common/hyperscan)

TIMEOUT(300)

SIZE(MEDIUM)

END()

ENDIF()
