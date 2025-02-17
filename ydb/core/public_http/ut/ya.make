UNITTEST_FOR(ydb/core/public_http)

SIZE(SMALL)

SRCS(
    http_router_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm14
)

YQL_LAST_ABI_VERSION()

END()
