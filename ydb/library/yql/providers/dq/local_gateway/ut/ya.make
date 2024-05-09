UNITTEST_FOR(ydb/library/yql/providers/dq/local_gateway)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/stub
    ydb/library/yql/sql/pg_dummy
    ydb/library/yql/dq/comp_nodes
    ydb/library/yql/dq/runtime
    ydb/library/yql/providers/dq/local_gateway
    ydb/library/yql/dq/transform
    ydb/library/yql/providers/common/comp_nodes
    ydb/library/yql/minikql
    ydb/library/yql/minikql/invoke_builtins/no_llvm
    ydb/library/yql/minikql/comp_nodes/no_llvm
    ydb/library/yql/minikql/computation/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
