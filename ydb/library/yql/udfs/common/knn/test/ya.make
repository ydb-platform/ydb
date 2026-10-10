YQL_UDF_TEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

DEPENDS(ydb/library/yql/udfs/common/knn)

END()
