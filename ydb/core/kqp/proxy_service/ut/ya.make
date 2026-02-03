UNITTEST_FOR(ydb/core/kqp/proxy_service)

FORK_SUBTESTS()

SIZE(MEDIUM)

SRCS(
    kqp_proxy_ut.cpp
    kqp_script_executions_ut.cpp
)

PEERDIR(
    library/cpp/protobuf/interop
    ydb/core/kqp/run_script_actor
    ydb/core/kqp/proxy_service
    ydb/core/kqp/ut/common
    ydb/core/kqp/workload_service/ut/common
    ydb/public/sdk/cpp/src/client/driver
    ydb/public/sdk/cpp/src/client/query
    ydb/services/ydb
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

END()
