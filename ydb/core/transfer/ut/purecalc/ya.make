UNITTEST()

SRCS(
    purecalc_memory_ut.cpp
)

PEERDIR(
    library/cpp/testing/unittest
    ydb/core/fq/libs/row_dispatcher/purecalc_no_pg_wrapper
    ydb/core/transfer
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/comp_nodes
    yql/essentials/minikql/comp_nodes/llvm16
    yql/essentials/minikql/computation
    yql/essentials/minikql/computation/llvm16
    yql/essentials/minikql/invoke_builtins
    yql/essentials/minikql/invoke_builtins/llvm16
    yql/essentials/public/udf/service/exception_policy
    yql/essentials/sql/pg_dummy
    yql/essentials/udfs/common/yson2
)

SIZE(MEDIUM)

YQL_LAST_ABI_VERSION()

END()
