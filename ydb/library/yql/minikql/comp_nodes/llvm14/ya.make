LIBRARY()

PEERDIR(
    ydb/library/yql/minikql/codegen/llvm14
    ydb/library/yql/minikql/invoke_builtins/llvm14
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm14/lib/Linker
    contrib/libs/llvm14/lib/Target/X86
    contrib/libs/llvm14/lib/Target/X86/AsmParser
    contrib/libs/llvm14/lib/Transforms/IPO
)

INCLUDE(../ya.make.inc)

END()

RECURSE_FOR_TESTS(
    ut
)
