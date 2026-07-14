UNITTEST_FOR(yt/yql/providers/dq/local_gateway)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    ydb/library/yql/dq/comp_nodes/no_llvm
    ydb/library/yql/dq/runtime
    ydb/library/yql/dq/transform
    yql/essentials/minikql
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/providers/common/comp_nodes
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    yt/yql/providers/dq/local_gateway
)

YQL_LAST_ABI_VERSION()

END()
