YQL_UDF_TEST()

DEPENDS(
    yql/essentials/udfs/common/hyperloglog
    yql/essentials/udfs/common/digest
)

TIMEOUT(300)

SIZE(MEDIUM)

END()
