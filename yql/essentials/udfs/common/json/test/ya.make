YQL_UDF_TEST()

DEPENDS(yql/essentials/udfs/common/json)

TIMEOUT(300)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
