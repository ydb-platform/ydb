UNITTEST()

SIZE(MEDIUM)
REQUIREMENTS(cpu:1)

FORK_SUBTESTS()

PEERDIR(
    contrib/restricted/nlohmann_json
    ydb/library/actors/http
    ydb/library/grpc/server
    ydb/library/grpc/server/actors
    ydb/core/base
    ydb/core/http_proxy
    ydb/core/testlib/default
    ydb/library/aclib
    ydb/library/persqueue/tests
    ydb/public/sdk/cpp/client/ydb_discovery
    ydb/public/sdk/cpp/client/ydb_types
    ydb/services/ydb
)

SRCS(
    ../http_proxy_ut.cpp
    inside_ydb_ut.cpp
)

RESOURCE(
    ydb/core/http_proxy/ut/internal_counters.json internal_counters.json
    ydb/core/http_proxy/ut/proxy_counters.json proxy_counters.json
)

ENV(INSIDE_YDB="1")

YQL_LAST_ABI_VERSION()

END()
