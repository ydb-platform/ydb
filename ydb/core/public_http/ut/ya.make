UNITTEST_FOR(ydb/core/public_http)

SIZE(SMALL)

SRCS(
    http_router_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
)

YQL_LAST_ABI_VERSION()

END()
