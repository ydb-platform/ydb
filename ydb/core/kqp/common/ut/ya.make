UNITTEST_FOR(ydb/core/kqp/common)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/grpc_services/local_rpc
    ydb/core/kqp/common
    ydb/core/kqp/topics
    ydb/core/kqp/ut/common
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/sql/pg_dummy
)

SRCS(
    kqp_query_request_ut.cpp
    kqp_shard_meta_ut.cpp
    kqp_tli_ut.cpp
    kqp_tx_manager_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
