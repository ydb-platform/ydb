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
    yql/essentials/public/udf/service/exception_policy
    ydb/library/yql/public/purecalc/common/no_llvm
    ydb/library/yql/providers/yt/codec/codegen/no_llvm
    yql/essentials/minikql/codegen/no_llvm
    yql/essentials/minikql/computation/no_llvm
    yql/essentials/minikql/invoke_builtins/no_llvm
    yql/essentials/minikql/comp_nodes/no_llvm
)

YQL_LAST_ABI_VERSION()

PROVIDES(YQL_PURECALC)

END()

