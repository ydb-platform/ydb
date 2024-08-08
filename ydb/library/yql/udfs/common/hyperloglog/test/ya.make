YQL_UDF_YDB_TEST()

DEPENDS(
    ydb/library/yql/udfs/common/hyperloglog
    ydb/library/yql/udfs/common/digest
)

TIMEOUT(300)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

END()
