UNITTEST_FOR(ydb/core/http_proxy)

SIZE(SMALL)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    ydb/core/http_proxy
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/public/sdk/cpp/client/ydb_types
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
)

SRCS(
    json_proto_conversion_ut.cpp
)

END()
