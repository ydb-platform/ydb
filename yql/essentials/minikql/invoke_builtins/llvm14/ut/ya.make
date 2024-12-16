UNITTEST()

SRCDIR(yql/essentials/minikql/invoke_builtins/llvm14)

PEERDIR(
    yql/essentials/minikql/codegen/llvm14
    yql/essentials/minikql/invoke_builtins/llvm14
    yql/essentials/minikql/computation/llvm14
    contrib/libs/llvm14/lib/IR
    contrib/libs/llvm14/lib/ExecutionEngine/MCJIT
    contrib/libs/llvm14/lib/Linker
    contrib/libs/llvm14/lib/Target/X86
    contrib/libs/llvm14/lib/Target/X86/AsmParser
    contrib/libs/llvm14/lib/Transforms/IPO
)

INCLUDE(../../ut/ya.make.inc)

END()