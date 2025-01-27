UNITTEST_FOR(yql/essentials/providers/pure)

SIZE(MEDIUM)

SRCS(
    yql_pure_provider_ut.cpp
)

PEERDIR(
    yql/essentials/core/facade
    yql/essentials/public/result_format
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/sql/pg_dummy
    yql/essentials/public/udf/service/exception_policy
    library/cpp/yson/node
)

YQL_LAST_ABI_VERSION()

END()
