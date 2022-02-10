UNITTEST_FOR(ydb/library/yql/udfs/common/topfreq/static)

OWNER(
    vmakeev
    g:yql 
)

SRCS(
    ../topfreq_udf_ut.cpp
)

PEERDIR(
    ydb/library/yql/minikql
    ydb/library/yql/minikql/comp_nodes
    ydb/library/yql/minikql/computation
    ydb/library/yql/public/udf/service/exception_policy
)

YQL_LAST_ABI_VERSION()

END()
