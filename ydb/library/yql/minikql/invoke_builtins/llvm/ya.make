LIBRARY()

NO_COMPILER_WARNINGS()

PEERDIR(
    ydb/library/yql/minikql/codegen/llvm
    ydb/library/yql/minikql/computation/llvm
    contrib/libs/llvm12/lib/IR
    contrib/libs/llvm12/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm12/lib/Linker
    contrib/libs/llvm12/lib/Target/X86
    contrib/libs/llvm12/lib/Target/X86/AsmParser
    contrib/libs/llvm12/lib/Transforms/IPO
)

INCLUDE(../ya.make.inc)

PROVIDES(mkql_invoke_builtins)

END()

RECURSE_FOR_TESTS(
    ut
)