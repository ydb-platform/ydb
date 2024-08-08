YQL_UDF_YDB_TEST()

TIMEOUT(600)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

DEPENDS(ydb/library/yql/udfs/common/histogram)

END()
