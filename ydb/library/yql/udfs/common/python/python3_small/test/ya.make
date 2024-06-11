YQL_UDF_YDB_TEST()

TIMEOUT(300)
SIZE(MEDIUM)

DEPENDS(
    ydb/library/yql/udfs/common/python/python3_small
)

END()
