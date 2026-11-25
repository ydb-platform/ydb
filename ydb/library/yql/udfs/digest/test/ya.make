YQL_UDF_MINITEST()

DEPENDS(
    ydb/library/yql/udfs/digest
    yql/essentials/udfs/common/digest/impl
)

DATA(
    arcadia/ydb/library/yql/udfs/digest/aggregate.sql
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
