YQL_UDF_MINITEST()

TIMEOUT(600)

SIZE(MEDIUM)

DEPENDS(
    yql/essentials/udfs/common/histogram/impl
    # XXX: Minirun expands HISTOGRAM(DISTINCT key) into Set UDF calls.
    yql/essentials/udfs/common/set/impl
)

END()
