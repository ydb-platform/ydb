UNITTEST_FOR(ydb/core/http_proxy)

SIZE(MEDIUM)

FORK_SUBTESTS()

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/path_aliasing/context
    ydb/public/sdk/cpp/src/client/driver
    contrib/restricted/nlohmann_json
    library/cpp/resource
    library/cpp/http/misc
    ydb/core/base
    ydb/core/http_proxy
    ydb/core/http_proxy/ut/datastreams_fixture
    ydb/core/testlib/default
    ydb/core/tx/datashard/ut_common
    ydb/library/aclib
    ydb/library/actors/http
    ydb/library/grpc/server
    ydb/library/grpc/server/actors
    ydb/library/persqueue/tests
    ydb/library/testlib/service_mocks
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    ydb/public/sdk/cpp/src/client/discovery
    ydb/public/sdk/cpp/src/client/types
    ydb/services/datastreams
    ydb/services/kesus
    ydb/services/persqueue_cluster_discovery
    ydb/services/ydb
    ydb/services/ymq
)

SRCS(
    path_aliasing_ut.cpp
    json_proto_conversion_ut.cpp
    http_ut.cpp
    utils_ut.cpp
)

RESOURCE(
    internal_counters.json internal_counters.json
    proxy_counters.json proxy_counters.json
)

YQL_LAST_ABI_VERSION()

END()

RECURSE_FOR_TESTS(
    inside_ydb_ut
    sqs_topic_ut
)

RECURSE(
    datastreams_fixture
)
