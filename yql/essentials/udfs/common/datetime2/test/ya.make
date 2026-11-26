YQL_UDF_MINITEST()

DEPENDS(
    yql/essentials/udfs/common/datetime2/impl
)

IF (SANITIZER_TYPE)
    TIMEOUT(1200)
    SIZE(LARGE)
    TAG(ya:fat)
ELSE()
    TIMEOUT(600)
    SIZE(MEDIUM)
ENDIF()

END()
