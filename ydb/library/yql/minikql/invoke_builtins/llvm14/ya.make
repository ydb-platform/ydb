LIBRARY()

PEERDIR(
    ydb/library/yql/minikql/codegen/llvm14
    ydb/library/yql/minikql/computation/llvm14
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm14/lib/Linker
    contrib/libs/llvm14/lib/Target/X86
    contrib/libs/llvm14/lib/Target/X86/AsmParser
    contrib/libs/llvm14/lib/Transforms/IPO
)

INCLUDE(../ya.make.inc)

PROVIDES(mkql_invoke_builtins)

END()

RECURSE_FOR_TESTS(
    ut
)
