YQL_UDF_TEST()

TIMEOUT(300)
SIZE(MEDIUM)

DEPENDS(
    yql/essentials/udfs/examples/tagged
)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

