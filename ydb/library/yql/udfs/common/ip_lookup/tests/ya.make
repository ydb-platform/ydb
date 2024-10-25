YQL_UDF_YDB_TEST()

TIMEOUT(300)

SIZE(MEDIUM)

IF (SANITIZER_TYPE == "memory")
    TAG(ya:not_autocheck) # YQL-15385
ENDIF()

DEPENDS(
    ydb/library/yql/udfs/common/ip_lookup
    ydb/library/yql/udfs/common/string
)

END()
