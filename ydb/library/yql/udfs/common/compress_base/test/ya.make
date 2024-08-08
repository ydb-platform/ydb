YQL_UDF_YDB_TEST()

DEPENDS(ydb/library/yql/udfs/common/compress_base)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

END()
