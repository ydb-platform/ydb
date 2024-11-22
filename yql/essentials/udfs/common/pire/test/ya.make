YQL_UDF_TEST_CONTRIB()

DEPENDS(yql/essentials/udfs/common/pire)

TIMEOUT(300)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
