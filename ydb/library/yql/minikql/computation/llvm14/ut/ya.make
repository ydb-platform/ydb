UNITTEST()

TAG(ya:manual)

PEERDIR(
    ydb/library/yql/minikql/codegen/llvm14
    ydb/library/yql/minikql/computation/llvm14
    ydb/library/yql/minikql/comp_nodes/llvm14
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm14/lib/Linker
    contrib/libs/llvm14/lib/Target/X86
    contrib/libs/llvm14/lib/Target/X86/AsmParser
    contrib/libs/llvm14/lib/Transforms/IPO
)

INCLUDE(../../ut/ya.make.inc)

END()
