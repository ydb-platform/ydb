UNITTEST_FOR(ydb/library/yql/providers/dq/local_gateway)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/dq/transform
    yql/essentials/providers/common/comp_nodes
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
