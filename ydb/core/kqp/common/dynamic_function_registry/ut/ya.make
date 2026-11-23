UNITTEST_FOR(ydb/core/kqp/common/dynamic_function_registry)

FORK_SUBTESTS()

SIZE(SMALL)

PEERDIR(
    ydb/core/kqp/common/dynamic_function_registry
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
)

SRCS(
    dynamic_function_registry_ut.cpp
)

YQL_LAST_ABI_VERSION()

END()
