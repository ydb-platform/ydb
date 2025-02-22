LIBRARY()

PEERDIR(
    yql/essentials/minikql/codegen/llvm16
    yql/essentials/minikql/computation/llvm16
    contrib/libs/llvm16/lib/IR
    contrib/libs/llvm16/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm16/lib/Linker
    contrib/libs/llvm16/lib/Passes
    contrib/libs/llvm16/lib/Target/X86
    contrib/libs/llvm16/lib/Target/X86/AsmParser
    contrib/libs/llvm16/lib/Target/X86/Disassembler
    contrib/libs/llvm16/lib/Transforms/IPO
    contrib/libs/llvm16/lib/Transforms/ObjCARC
)

INCLUDE(../ya.make.inc)

PROVIDES(mkql_invoke_builtins)

END()

RECURSE_FOR_TESTS(
    ut
)
