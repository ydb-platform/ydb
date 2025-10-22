LIBRARY()


PEERDIR(
    contrib/restricted/nlohmann_json
    library/cpp/resource
    library/cpp/http/misc
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
    datastreams_fixture.cpp
)

YQL_LAST_ABI_VERSION()

END()

