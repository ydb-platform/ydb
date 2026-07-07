UNITTEST_FOR(yt/yql/providers/dq/local_gateway)

SRCS(
    yql_dq_gateway_local_ut.cpp
)

PEERDIR(
    yql/essentials/public/udf/service/stub
    yql/essentials/sql/pg_dummy
    contrib/ydb/library/yql/dq/comp_nodes/no_llvm
    contrib/ydb/library/yql/dq/runtime
    yt/yql/providers/dq/local_gateway
    contrib/ydb/library/yql/dq/transform
    yql/essentials/providers/common/comp_nodes
    yql/essentials/minikql
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
    yql/essentials/minikql/computation/no_llvm
)

YQL_LAST_ABI_VERSION()

END()
