UNITTEST_FOR(ydb/core/kqp/common)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/kqp/common
    ydb/core/kqp/ut/common
    ydb/core/kqp/topics
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/sql/pg_dummy
)

SRCS(
    dynamic_function_registry_ut.cpp
    kqp_shard_meta_ut.cpp
    kqp_tli_ut.cpp
    kqp_tx_manager_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
