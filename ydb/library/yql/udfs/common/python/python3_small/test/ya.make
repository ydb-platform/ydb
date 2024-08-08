YQL_UDF_YDB_TEST()

TIMEOUT(300)
SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

DEPENDS(
    ydb/library/yql/udfs/common/python/python3_small
)

END()
