LIBRARY()

ADDINCL(
   ydb/library/yql/public/purecalc
)

SRCDIR(
   ydb/library/yql/public/purecalc
)

SRCS(
    purecalc.cpp
)

PEERDIR(
    ydb/library/yql/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common/no_llvm
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    ydb/library/yql/minikql/codegen/no_llvm
    ydb/library/yql/minikql/computation/no_llvm
    ydb/library/yql/minikql/invoke_builtins/no_llvm
    ydb/library/yql/minikql/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

PROVIDES(YQL_PURECALC)

END()

