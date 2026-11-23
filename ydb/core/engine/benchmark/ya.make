G_BENCHMARK()

PEERDIR(
    ydb/core/engine
    ydb/core/kqp/common
    library/cpp/random_provider
    library/cpp/time_provider
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

YQL_LAST_ABI_VERSION()

ENV(MKQL_SHARE_SHARDED_PROGRAM=1)

SRCS(
    mkql_engine_flat_bench.cpp
)

END()
