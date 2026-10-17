YQL_UDF_TEST()

DEPENDS(
    yql/essentials/udfs/common/hyperloglog/impl
    yql/essentials/udfs/common/digest/impl
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
