YQL_UDF_YDB_TEST()

TIMEOUT(300)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/library/yql/udfs/examples/tagged
)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()

