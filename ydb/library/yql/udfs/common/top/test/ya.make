YQL_UDF_YDB_TEST()

DEPENDS(ydb/library/yql/udfs/common/top)

TIMEOUT(300)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
