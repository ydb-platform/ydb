LIBRARY()

SRCS(
    purecalc.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common
    ydb/library/yql/providers/yt/codec/codegen
    ydb/library/yql/minikql/computation/llvm
    ydb/library/yql/minikql/invoke_builtins/llvm
    ydb/library/yql/minikql/comp_nodes/llvm
)

YQL_LAST_ABI_VERSION()

PROVIDES(YQL_PURECALC)

END()

RECURSE(
    common
    examples
    helpers
    io_specs
    no_llvm
)

RECURSE_FOR_TESTS(
    ut
)
