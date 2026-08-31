UNITTEST_FOR(ydb/core/public_http)

SIZE(SMALL)

SRCS(
    http_router_ut.cpp
)

PEERDIR(
    ydb/public/lib/ydb_cli/dump/util/view_query_dummy
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    yql/essentials/minikql/comp_nodes/llvm16
)

YQL_LAST_ABI_VERSION()

END()
