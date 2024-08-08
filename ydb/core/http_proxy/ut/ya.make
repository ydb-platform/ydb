UNITTEST_FOR(ydb/core/http_proxy)

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    ydb/core/base
    ydb/core/http_proxy
    ydb/core/testlib/default
    ydb/core/tx/datashard/ut_common
    ydb/library/aclib
    ydb/library/actors/http
    ydb/library/grpc/server
    ydb/library/grpc/server/actors
    ydb/library/persqueue/tests
    ydb/library/testlib/service_mocks
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/sql/pg_dummy
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_types
    ydb/services/datastreams
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
)

SRCS(
    json_proto_conversion_ut.cpp
    datastreams_fixture.h
)

RESOURCE(
    internal_counters.json internal_counters.json
    proxy_counters.json proxy_counters.json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    inside_ydb_ut
)
