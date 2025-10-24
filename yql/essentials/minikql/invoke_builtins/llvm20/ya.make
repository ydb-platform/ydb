LIBRARY()

PEERDIR(
    yql/essentials/minikql/codegen/llvm20
    yql/essentials/minikql/computation/llvm20
    contrib/libs/llvm20/lib/IR
    contrib/libs/llvm20/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm20/lib/Linker
    contrib/libs/llvm20/lib/Passes
    contrib/libs/llvm20/lib/Target/X86
    contrib/libs/llvm20/lib/Target/X86/AsmParser
    contrib/libs/llvm20/lib/Target/X86/Disassembler
    contrib/libs/llvm20/lib/Transforms/IPO
    contrib/libs/llvm20/lib/Transforms/ObjCARC
)

INCLUDE(../ya.make.inc)

PROVIDES(mkql_invoke_builtins)

END()

RECURSE_FOR_TESTS(
    ut
)
