YQL_UDF_TEST()

DEPENDS(yql/essentials/udfs/common/compress_base)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

SIZE(MEDIUM)

END()
