UNITTEST()

PEERDIR(
    yql/essentials/minikql/codegen/llvm20
    yql/essentials/minikql/computation/llvm20
    yql/essentials/minikql/comp_nodes/llvm20
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

INCLUDE(../../ut/ya.make.inc)

END()
